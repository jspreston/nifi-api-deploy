import groovy.json.JsonBuilder
import groovyx.net.http.RESTClient
import org.apache.http.entity.mime.MultipartEntity
import org.apache.http.entity.mime.content.StringBody
import org.yaml.snakeyaml.Yaml

import java.util.logging.Logger

import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.Method.POST

@Grab(group='org.codehaus.groovy.modules.http-builder',
        module='http-builder',
        version='0.7.1')
@Grab(group='org.yaml',
        module='snakeyaml',
        version='1.17')
@Grab(group='org.apache.httpcomponents',
        module='httpmime',
        version='4.2.1')

// see actual script content at the bottom of the text,
// after every implementation method. Groovy compiler likes these much better

def cli = new CliBuilder(usage: 'groovy NiFiDeploy.groovy [options]',
        header: 'Options:')
cli.with {
  f longOpt: 'file',
          'Deployment specification file in a YAML format',
          args:1, argName:'name', type:String.class
  h longOpt: 'help', 'This usage screen'
  d longOpt: 'debug', 'Debug underlying HTTP wire communication'
  n longOpt: 'nifi-api', 'NiFi REST API (override), e.g. http://example.com:9090',
          args:1, argName:'http://host:port', type:String.class
  c longOpt: 'client-id', 'Client ID for API calls, any unique string (override)',
          args:1, argName:'id', type:String.class
  w longOpt: 'waittime', 'maximum wait time for process/service state change',
          args:1, argName:'time (ms)', type:Integer.class
}

def opts = cli.parse(args)
if (!opts) { return }
if (opts.help) {
  cli.usage()
  return
}

maxWaitMs = opts.waittime ?: 30000

def deploymentSpec
if (opts.file) {
  deploymentSpec = opts.file
} else {
  log.error "Missing a file argument\n"
  cli.usage()
  System.exit(-1)
}

if (opts.debug) {
  System.setProperty('org.apache.commons.logging.Log', 'org.apache.commons.logging.impl.SimpleLog')
  System.setProperty('org.apache.commons.logging.simplelog.showdatetime', 'true')
  System.setProperty('org.apache.commons.logging.simplelog.log.org.apache.http', 'DEBUG')
}

// implementation methods below

/**
 * @param templateUri
 * @return map containing template name, list of top-level process groups in template, and list of top-level
 * controller services in template.
 */
def parseTemplate(templateUri) {
  log.info "URI: ${templateUri}"
  def t = new XmlSlurper().parse(templateUri)
  def templateName = t.name.text()
  log.info "template name: ${templateName}"
  log.info "Number of process groups: ${t.snippet.processGroups.size()}"
  def topLevelProcessGroups = t.snippet.processGroups.collect { pg -> pg.name.text() }
  log.info "Number of controller services: ${t.snippet.controllerServices.size()}"
  def topLevelControllerServices = t.snippet.controllerServices.collect{ cs -> cs.name.text() }
  assert !t.snippet.processors.size() : "Cannot handle top-level processors"
  [
          templateName: templateName,
          processGroups: topLevelProcessGroups,
          controllerServices: topLevelControllerServices
  ]
}

def undeployTemplate(templateName) {
  def t = lookupTemplate(templateName)
  if (t) {
    def resp = nifi.delete(
            path: "templates/$t.id"
    )
    assert resp.status == 200
    log.info "Deleted template : $templateName"
  }else{
    log.warn "no template ${templateName} found to undeploy"
  }
}

/**
 * Delete top-level controller services specified as a list of names
 * @param csList list of top-level controller services
 */
def undeployControllerServices(csList) {
  def controllerServices = new ControllerServiceLookup(nifi)

  csList.each { csName ->
    def cs = controllerServices.findByName(csName)
    log.info "Undeploying Process Group: $csName"
    if (cs) {
      stopControllerService(cs.id)

      def resp_get = nifi.get(
              path: "controller-services/$cs.id"
      )

      def resp = nifi.delete(
              path: "controller-services/$cs.id",
              query: [
                      clientId: client,
                      version : resp_get.data.revision.version
              ]
      )
      assert resp.status == 200
    } else {
      log.warn "null controller service encountered"
    }
  }
}

/**
 * Delete top-level process groups specified as a list of names
 * @param pgList
 * @return
 */
def undeployProcessGroups(pgList) {

  def processGroups = new ProcessGroupLookup(nifi)

  pgSaveData = pgList.collect { pgName ->
    log.info "Undeploying Process Group: $pgName"

    // getting pgId
    def pg = processGroups.findByName(pgName)

    def pgId = pg.id

    stopProcessGroup(pgId)

    def connData = deletePgConnections(pgId)

    // now delete it
    resp = nifi.delete(
            path: "process-groups/$pgId",
            query: [
                    clientId: client,
                    version: pg.revision.version
            ]
    )
    log.info "Deleted process group ${pgName} at position ${pg.position}"
    assert resp.status == 200

    return [
            position: pg.position,
            connections: connData
    ]
  }
  return pgSaveData
}

/**
 * @param name of the template
 * @return a json-backed template structure from NiFi. Null if not found.
 */
def lookupTemplate(String name) {
  def resp = nifi.get(
          path: 'flow/templates'
  )
  assert resp.status == 200

  if (resp.data.templates.template.name.grep(name).isEmpty()) {
    return null
  }

  assert resp.data.templates.template.name.grep(name).size().toInteger() == 1 :
          "Multiple templates found named '$name'"

  def t = resp.data.templates.find { it.template.name == name }
  assert t != null

  return t
}

def uploadTemplate(String templateUri) {
  log.info "Loading template from URI: $templateUri"
  def t = new XmlSlurper().parse(templateUri)
  assert t : "Error, couldn't find template file ${templateUri}"
  templateName = t.name.text()
  assert templateName : "Error, couldn't find template name in ${templateUri}"
  undeployTemplate(templateName)

  def templateUrlString = templateUri.toURL().text

  nifi.request(POST) { request ->
    uri.path = 'process-groups/root/templates/upload'

    requestContentType = 'multipart/form-data'
    MultipartEntity entity = new MultipartEntity()
    entity.addPart("template", new StringBody(templateUrlString))
    request.entity = entity

    response.success = { resp, xml ->
      switch (resp.statusLine.statusCode) {
        case 200:
          throw new RuntimeException("template already exists!")
          break
        case 201:
          // grab the trailing UUID part of the location URL header
          templateId = xml.template.id.text()
          log.info "Template successfully imported into NiFi. ID: $templateId"
          return templateId
        default:
          throw new Exception("Error importing template")
          break
      }
    }
  }
}

def instantiateTemplate(String id, Map pos=null) {
  pos = pos ?: [x: 0, y: 0]
  assert pos.keySet() == ['x', 'y'].toSet() : "pos should be map containing keys x and y"
  def resp = nifi.post (
          path: 'process-groups/root/template-instance',
          body: [
                  templateId: id,
                  originX: pos.x,
                  originY: pos.y
          ],
          requestContentType: JSON
  )

  assert resp.status == 201
}

class ProcessGroupLookup {

  def processGroups
  def parentId

  ProcessGroupLookup(conn, parentId='root'){
    this.parentId = parentId
    def resp = conn.get(
            path: "process-groups/${parentId}/process-groups"
    )
    assert resp.status == 200
    this.processGroups = resp.data.processGroups
  }

  def findByName(name) {
    def pgs = this.processGroups.findAll { it.component.name == name }
    assert pgs.size() != 0 : "Processing Group '${name}' not found in " +
            "processGroup ${parentId}, check your deployment config?"
    assert pgs.size() == 1 : "Ambiguous Processing Group '${name}' in processGroup ${parentId}!"
    return pgs[0]
  }
}

class ProcessorLookup {

  def processors
  def parentId

  ProcessorLookup(conn, parentId='root'){
    this.parentId = parentId
    def resp = conn.get(
            path: "process-groups/${parentId}/processors"
    )
    assert resp.status == 200
    this.processors = resp.data.processors
  }

  def findByName(name) {
    def proc = this.processors.findAll { it.component.name == name }
    assert proc.size() != 0 : "Processor '${name}' not found in " +
            "processGroup ${parentId}, check your deployment config?"
    assert proc.size() == 1 : "Ambiguous Processor '${name}' in processGroup ${parentId}!"
    return proc[0]
  }
}

class ControllerServiceLookup {

  def controllers
  def parentId

  ControllerServiceLookup(conn, parentId='root'){
    this.parentId = parentId
    def resp = conn.get(
            path: "flow/process-groups/${parentId}/controller-services"
    )
    assert resp.status == 200
    this.controllers = resp.data.controllerServices
  }

  def findByName(name) {
    def cs = this.controllers.findAll { it.component.name == name }
    assert cs.size() != 0 : "Controller service '${name}' not found in " +
            "processGroup ${parentId}, check your deployment config?"
    assert cs.size() == 1 : "Ambiguous controller service '${name}' in processGroup ${parentId}!"
    return cs[0]
  }
}

/**
 * Traverse a hierarchical config structure, calling func for each ProcessGroup encountered
 * @param config config-file structure
 * @param func func accepting a single argument
 * @return
 */
def traverseProcessGroups(config, func) {
  def resp = nifi.get(
          path: "process-groups/root"
  )
  assert resp.status == 200
  // convert to Map.Entry with key 'root'
  def pgConfig = ['root': config].entrySet().first()
  pgData = [pg: resp.data, config: pgConfig, level: 0, parent: null]
  _traverseProcessGroups(pgData, func)
}

def _traverseProcessGroups(pgData, func) {
  func(pgData)
  def processGroups = new ProcessGroupLookup(nifi, pgData.pg.id)
  pgData.config.value.processGroups.each { subPgConfig ->
    def subPg = processGroups.findByName(subPgConfig.key)
    subPgData = [pg: subPg, config: subPgConfig, level: pgData.level+1, parent: pgData.config.key]
    _traverseProcessGroups(subPgData, func)
  }
}

def traverseProcessors(config, func) {
  traverseProcessGroups(config, {
    pgData ->
      def processors = new ProcessorLookup(nifi, pgData.pg.id)
      pgData.config.value.processors.each { curProcConfig ->
        def curProcName = curProcConfig.key
        def curProc = processors.findByName(curProcName)
        def procData = [proc: curProc, config: curProcConfig, level: pgData.level, parent: pgData.config.key]
        func(procData)
      }
  })
}

def traverseControllerServices(config, func) {
  traverseProcessGroups(config, {
    pgData ->
      def services = new ControllerServiceLookup(nifi, pgData.pg.id)
      pgData.config.value.controllerServices.each { curCsConfig ->
        def curCsName = curCsConfig.key
        def curCs = services.findByName(curCsName)
        def csData = [cs: curCs, config: curCsConfig, level: pgData.level, parent: pgData.config.key]
        func(csData)
      }
  })
}

def configureProcessor(proc, procConfig) {
  def existingComments = proc.component.config.commments
  def procName = procConfig.key

  log.info "Stopping Processor '$procName' ($proc.id)"
  stopProcessor(proc.component.parentGroupId, proc.id)

  def procProps = procConfig?.value?.config?.entrySet() ?: []

  if (procProps.size() > 0) log.info "Configuring Processor '$procName' ($proc.id)"

  def rootControllerServices = new ControllerServiceLookup(nifi, 'root')

  def resp_get = nifi.get (
          path: "processors/$proc.id"
  )

  def builder = new JsonBuilder()
  builder {
    revision {
      clientId client
      version resp_get.data.revision.version
    }
    component {
      id proc.id
      config {
        comments existingComments ?: defaultComment
        properties {
          procProps.each { p ->
            // check if it's a ${referenceToControllerServiceName}
            def ref = p.value =~ /\$\{(.*)}/
            if (ref) {
              def name = ref[0][1] // grab the first capture group (nested inside ArrayList)
              // lookup the CS by name and get the newly generated ID instead of the one in a template
              def newCS = rootControllerServices.findByName(name)
              assert newCS : "Couldn't locate Controller Service with the name: $name"
              "$p.key" newCS.id
            } else {
              "$p.key" p.value
            }
          }
        }
      }
    }
  }

  def resp = nifi.put (
          path: "processors/$proc.id",
          body: builder.toPrettyString(),
          requestContentType: JSON
  )
  assert resp.status == 200

  // check if pgConfig tells us to start this processor
  if(procConfig.value.state != null){
    assert procConfig.value.state in ['RUNNING', 'STOPPED'] :
            "Invalid state ${procConfig.value.state} for processor ${procConfig.key}"
    if (procConfig.value.state == 'RUNNING') {
      log.info "Will start it up next"
      startProcessor(proc.component.parentGroupId, proc.id)
    }
  }
}

def configureProcessGroup(pg, pgConfig) {

  log.info "Configuring Process Group: $pgConfig.key ($pg.id)"

  def builder = new JsonBuilder()

  // if no comment is set, update process group comments with a deployment timestamp
  if (!pg.component.comments) {
    builder {
      revision {
        clientId client
        version pg.revision.version
      }
      id pg.id
      component {
        id pg.id
        comments defaultComment
      }
    }

    resp = nifi.put (
            path: "process-groups/$pg.id",
            body: builder.toPrettyString(),
            requestContentType: JSON
    )
    assert resp.status == 200
  }

}

def configureControllerService(cs, Map.Entry cfg) {
  def name = cfg.key
  cs = cs.component
  log.info "Found the controller service '$cs.name'. Current state is ${cs.state}."

  if (cfg.value?.config) {
    log.info "Applying controller service '$cs.name' configuration"

    def resp_get = nifi.get (
            path: "controller-services/$cs.id"
    )

    def builder = new JsonBuilder()
    builder {
      revision {
        clientId client
        version resp_get.data.revision.version
      }
      component {
        id cs.id
        comments cs.comments ?: defaultComment
        properties {
          cfg.value.config.each { p ->
            "$p.key" p.value
          }
        }
      }
    }

    resp = nifi.put (
            path: "controller-services/$cs.id",
            body: builder.toPrettyString(),
            requestContentType: JSON
    )
    assert resp.status == 200
  }


  log.info "Enabling $cs.name (${cs.id})"
  startControllerService(cs.id)
}

/**
 * Get the input and/or output ports for a process group
 * @param pgId the id of the process group
 * @param connType one of 'output', 'input', or 'any'
 * @return a mapping from port name to port id
 */
def getPgPorts(pgId, connType) {

  assert connType in ['output', 'input', 'any'] : "connType not recognized"

  def portData = []
  if (connType == 'output' || connType == 'any') {
    def resp_get = nifi.get(
            path: "process-groups/${pgId}/output-ports"
    )
    portData.addAll(resp_get.data.outputPorts)
    assert resp_get.status == 200
  }
  if (connType == 'input' || connType == 'any') {
    def resp_get = nifi.get(
            path: "process-groups/${pgId}/input-ports"
    )
    portData.addAll(resp_get.data.inputPorts)
    assert resp_get.status == 200
  }
  portData.collectEntries { p -> [(p.component.name): p.component.id] }
}

/**
 * Get a list of incoming or outgoing connections for a process group.  Determines connections by looking at what
 * connects to process group's input and output ports.
 * @param pgId the id of the process group
 * @param connType one of 'output', 'input', or 'any'
 * @return a mapping from port name to list of connections to/from this process group
 */
def getPgConnections(pgId, connType) {
  def resp_get = nifi.get (
          path: "process-groups/root/connections"
  )
  def ports = getPgPorts(pgId, connType)
  ports.collectEntries { name, portId ->
    def connList = resp_get.data.connections.findAll { c ->
      switch (connType) {
        case 'output':
          return c.component.source.id == portId
        case 'input':
          return c.component.destination.id == portId
        case 'any':
          return c.component.source.id == portId || c.component.destination.id == portId
        default:
          throw new IllegalArgumentException('connType must be output, input, or any')
        }
    }
    log.info "found ${connList.size()} connections to/from port ${name}"
    return [(name): connList]
  }
}

/**
 * Delete incoming and outgoing connections to/from the specified process group
 * @param pgId the ID of the process group
 * @return
 */
def deletePgConnections(pgId){
  def connections = getPgConnections(pgId, 'any')
  connections.each { name, outConns ->
    outConns.each { conn ->
      def resp = nifi.get (
              path: "connections/${conn.component.id}"
      )
      assert resp.status == 200
      def connData = resp.data
      if (connData.component.source.running) {
        stopProcessGroup(connData.component.source.groupId)
      }
      if (connData.component.destination.running) {
        stopProcessGroup(connData.component.destination.groupId)
      }
      resp = nifi.delete (
              path: "connections/${conn.component.id}",
              query: resp.data.revision
      )
      log.info "removed connection from  ${connData.component.source.name} to ${connData.component.destination.name}"
      assert resp.status == 200
    }
  }
}


def instantiateConnection(connConfig) {
  assert connConfig?.source?.name : "connection source name must be specified"
  assert connConfig?.destination?.name : "connection destination name must be specified"
  assert connConfig?.source?.port : "connection source port must be specified"
  assert connConfig?.destination?.port : "connection destination port must be specified"

  def processGroups = new ProcessGroupLookup(nifi)
  def pgSource = processGroups.findByName(connConfig.source.name)
  def pgDestination = processGroups.findByName(connConfig.destination.name)

  def pgSourceOutputPorts = getPgPorts(pgSource.id, 'output')
  def pgDestinationInputPorts = getPgPorts(pgDestination.id, 'input')

  def pn = connConfig.source.port
  def sourceOutputPortId = pgSourceOutputPorts[pn]
  def destinationInputPortId = pgDestinationInputPorts[connConfig.destination.port]

  assert sourceOutputPortId : "unable to find output port '${connConfig.source.port}' in process group " +
          "${connConfig.source.name}, options are ${pgSourceOutputPorts.keySet() as String[]}"
  assert destinationInputPortId : "unable to find output port '${connConfig.destination.port}' in process group " +
          "${connConfig.destination.name}, options are ${pgDestinationInputPorts.keySet() as String[]}"

  def builder = new JsonBuilder()
  builder {
    revision {
      version 0
    }
    component {
      source {
        id sourceOutputPortId
        groupId pgSource.id
        type 'OUTPUT_PORT'
      }
      destination {
        id destinationInputPortId
        groupId pgDestination.id
        type 'INPUT_PORT'
      }
    }
  }

  def resp = nifi.post (
          path: "process-groups/root/connections",
          body: builder.toPrettyString(),
          requestContentType: JSON
  )
  switch(resp.status) {
    case 201:
      break;
    case 200:
      log.warn "connection already exists"
      break
    default:
      throw new RuntimeException("Error response from connection creation")
  }

}


def stopProcessor(processGroupId, processorId) {
  _changeProcessorState(processGroupId, processorId, false)
}

def startProcessor(processGroupId, processorId) {
  _changeProcessorState(processGroupId, processorId, true)
}

private _changeProcessorState(processGroupId, processorId, boolean running) {

  resp_get = nifi.get (
          path: "processors/$processorId"
  )

  def builder = new JsonBuilder()
  builder {
    revision {
      clientId client
      version resp_get.data.revision.version
    }
    id processorId
    component {
      id processorId
      state running ? 'RUNNING' : 'STOPPED'
    }
  }


  resp = nifi.put (
          path: "processors/$processorId",
          body: builder.toPrettyString(),
          requestContentType: JSON
  )
  assert resp.status == 200
}

def startProcessGroup(pgId) {
  _changeProcessGroupState(pgId, "RUNNING")
}

def stopProcessGroup(pgId) {
  print "Waiting for a Process Group to stop: $pgId "
  _changeProcessGroupState(pgId, "STOPPED")


  def resp = nifi.get(path: "process-groups/$pgId")
  assert resp.status == 200
  long start = System.currentTimeMillis()

  // keep polling till active threads shut down, but no more than maxWaitMs time
  while ((System.currentTimeMillis() < (start + maxWaitMs)) &&
          resp.data.runningCount > 0) {
    sleep(1000)
    resp = nifi.get(path: "process-groups/$pgId")
    assert resp.status == 200
    print '.'
  }
  if (resp.data.runningCount == 0) {
    log.info 'Done'
  } else {
    log.warn "Failed to stop the processing group, request timed out after ${maxWaitMs/1000} seconds"
    System.exit(-1)
  }
}

private _changeProcessGroupState(pgId, String state) {
  def resp = nifi.put(
          path: "flow/process-groups/$pgId",
          body: [
                  id: pgId,
                  state: state
          ],
          requestContentType: JSON
  )
  assert resp.status == 200
}

def stopControllerService(csId) {
  _changeControllerServiceState(csId, false)
}

def startControllerService(csId) {
  _changeControllerServiceState(csId, true)
}

def _changePortState(portId, connType, newState) {

  def resp = nifi.get(path: "${connType}-ports/${portId}")
  assert resp.status == 200

  JsonBuilder builder = new JsonBuilder()
  builder {
    revision {
      version resp.data.revision.version
    }
    id portId
    component {
      id portId
      state newState
    }
  }

  resp = nifi.put(
          path: "${connType}-ports/${portId}",
          body: builder.toPrettyString(),
          requestContentType: JSON
  )
  assert resp.status == 200

}

def startPort(portId, connType) {
  _changePortState(portId, connType, 'RUNNING')
}

private _changeControllerServiceState(csId, boolean enabled) {


  def resp_get = nifi.get (
          path: "controller-services/$csId"
  )

  def test = resp_get.data.component.referencingComponents.collect{[(it.id): it.revision]}.collectEntries{ it }

  if (!enabled) {
    // gotta stop all CS references first when disabling a CS
    def resp = nifi.put (
            path: "controller-services/$csId/references",
            body: [
                    referencingComponentRevisions: test,
                    id: csId,
                    state: 'STOPPED'
            ],
            requestContentType: JSON
    )
    assert resp.status == 200
  }

  def builder = new JsonBuilder()
  builder {
    revision {
      clientId client
      version resp_get.data.revision.version
    }
    component {
      id csId
      state enabled ? 'ENABLED' : 'DISABLED'
    }
  }

  resp = nifi.put(
          path: "controller-services/$csId",
          body: builder.toPrettyString(),
          requestContentType: JSON
  )
  assert resp.status == 200
}

def startProcessGroupPorts(pgId) {
  ['input', 'output'].each { connType ->
    portMap = getPgPorts(pgId, connType)
    portMap.each { name, id ->
      log.info "enabling ${connType} port ${name}: ${id}"
      startPort(id, connType)
    }
  }
}

def setProcessGroupState(pgName, pgId, state) {
  if (state == null) return

  assert state in ['RUNNING', 'STOPPED', 'PORTS-RUNNING'] :
          "Invalid state ${state} requested for Process Group ${pgName}"
  switch (state) {
    case 'RUNNING':
      log.info "Starting Process Group: $pgName ($pgId)"
      startProcessGroup(pgId)
      break
    case 'PORTS-RUNNING':
        log.info "Starting Process Group Ports: $pgName ($pgId)"
      startProcessGroupPorts(pgId)
      break
  }
}

def testTraversal(pgConfig) {
  println '#### TRAVERSING PROCESS GROUPS ####'
  traverseProcessGroups(pgConfig, {
    pgData ->
      println( ("----"*pgData.level) +
              "found process group ${pgData.pg.component.name} = ${pgData.config.key}, id ${pgData.pg.id}")
  })

  println '#### TRAVERSING PROCESSORS ####'
  traverseProcessors(pgConfig, {
    procData ->
      println( ("----"*procData.level) +
              "found processor ${procData.proc.component.name} = ${procData.config.key}, id ${procData.proc.id}")
  })

  println '#### TRAVERSIGN CONTROLLER SERVICES ####'
  traverseControllerServices(pgConfig, {
    csData ->
      println( ("----"*csData.level) +
              "found controller service ${csData.cs.component.name} = ${csData.config.key}, id ${csData.cs.id}")
  })
}

// script flow below
log = Logger.getLogger("NiFiDeploy")

conf = new Yaml().load(new File(deploymentSpec).text)
assert conf

def nifiHostPort = opts.'nifi-api' ?: conf.nifi.url
if (!nifiHostPort) {
  log.error 'Please specify a NiFi instance URL in the deployment spec file or via CLI'
  System.exit(-1)
}
nifiHostPort = nifiHostPort.endsWith('/') ? nifiHostPort[0..-2] : nifiHostPort
assert nifiHostPort : "No NiFI REST API endpoint provided"

nifi = new RESTClient("$nifiHostPort/nifi-api/")
nifi.handler.failure = { resp, data ->
  resp.setData(data?.text)
  log.error "HTTP call failed. Status code: $resp.statusLine: $resp.data"
  // fail gracefully with a more sensible groovy stacktrace
  assert null : "Terminated script execution"
}

client = opts.'client-id' ?: conf.nifi.clientId
assert client : 'Client ID must be provided'

thisHost = InetAddress.localHost
defaultComment = "Last updated by '$client' on ${new Date()} from $thisHost"

if (opts.debug) testTraversal(conf)

templateData = conf.templates.collect { parseTemplate(it.uri) }
templateNames = templateData.collect { it.templateName }
processGroups = templateData.collect { it.processGroups } .flatten()
controllerServices = templateData.collect {it.controllerServices } .flatten()

log.info "undeploying process groups ${processGroups}"
undeployProcessGroups(processGroups)

log.info "undeploying controller services ${controllerServices}"
undeployControllerServices(controllerServices)

conf.templates.each {templateConfig ->
  def templateId = uploadTemplate(templateConfig.uri)
  log.info "instantiating templateId ${templateId}"
  instantiateTemplate(templateId, templateConfig?.position)
}

log.info "Configuring Process Groups"
traverseProcessGroups(conf, {pgData -> configureProcessGroup(pgData.pg, pgData.config) })
log.info "Configuring Controller Services"
traverseControllerServices(conf, {csData -> configureControllerService(csData.cs, csData.config)})
log.info "Configuring Processors"
traverseProcessors(conf, {procData -> configureProcessor(procData.proc, procData.config) })

log.info "Instantiating Connections"
conf.connections.each { conn -> instantiateConnection(conn) }

// Start process groups as requested.  For top-level groups to start correctly, connections must have already been
// instantiated
log.info "Starting process groups"
traverseProcessGroups(conf, {
  pgData ->
    setProcessGroupState(pgData.config.key, pgData.pg.id, pgData.config.value.state)
})

log.info 'All Done.'
