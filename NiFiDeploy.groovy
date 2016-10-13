import groovy.json.JsonBuilder
import groovyx.net.http.RESTClient
import org.apache.http.entity.mime.MultipartEntity
import org.apache.http.entity.mime.content.StringBody
import org.yaml.snakeyaml.Yaml

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson
import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.ContentType.URLENC
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
  t longOpt: 'template', 'Template URI (override)',
          args:1, argName:'uri', type:String.class
  c longOpt: 'client-id', 'Client ID for API calls, any unique string (override)',
          args:1, argName:'id', type:String.class
}

def opts = cli.parse(args)
if (!opts) { return }
if (opts.help) {
  cli.usage()
  return
}


def deploymentSpec
if (opts.file) {
  deploymentSpec = opts.file
} else {
  println "ERROR: Missing a file argument\n"
  cli.usage()
  System.exit(-1)
}

if (opts.debug) {
  System.setProperty('org.apache.commons.logging.Log', 'org.apache.commons.logging.impl.SimpleLog')
  System.setProperty('org.apache.commons.logging.simplelog.showdatetime', 'true')
  System.setProperty('org.apache.commons.logging.simplelog.log.org.apache.http', 'DEBUG')
}

// implementation methods below

def handleUndeploy() {
  if (!conf.nifi.undeploy) {
    return
  }
  processGroups = loadProcessGroups()

  // delete templates
  // stop & remove controller services
  // stop & remove process groups


  conf.nifi?.undeploy?.templates?.each { tName ->
    println "Deleting template: $tName"
    def t = lookupTemplate(tName)
    if (t) {
      def resp = nifi.delete(
              path: "templates/$t.id"
      )
      assert resp.status == 200
      println "Deleted template : $tName"
    }
  }


  conf.nifi?.undeploy?.processGroups?.each { pgConfig ->
    pgName = pgConfig.key
    println "Undeploying Process Group: $pgName"

    // getting pgId
    def pg = processGroups.findAll { it.component.name == pgName }
    if (pg.isEmpty()) {
      println "[WARN] No such process group found in NiFi"
      return
    }
    assert pg.size() == 1 : "Ambiguous process group name"

    def pgId = pg[0].id

    pgConfig.value.controllerServices.each { csName ->
      print "Undeploying Controller Service: $csName"
      def cs = lookupControllerService(pgId, csName)
      if (cs) {
        println " ($cs.id)"
        stopControllerService(cs.id)

        def resp_get = nifi.get (
                path: "controller-services/$cs.id"
        )

        def resp = nifi.delete(
                path: "controller-services/$cs.id",
                query: [
                        clientId: client,
                        version: resp_get.data.revision.version
                ]
        )
        assert resp.status == 200
      } else {
        println ''
      }
    }

    stopProcessGroup(pgId)

    // now delete it
    resp = nifi.delete(
            path: "process-groups/$pgId",
            query: [
                    clientId: client,
                    version: pg[0].revision.version
            ]
    )
    println "[INFO] Deleted process group ${pgName}"
    assert resp.status == 200
  }

}

/**
 Returns a json-backed controller service structure from NiFi
 */
def lookupControllerService(pgId, name) {
  def resp = nifi.get(
          path: "flow/process-groups/${pgId}/controller-services"
  )
  assert resp.status == 200

  if (resp.data.controllerServices.component.name.grep(name).isEmpty()) {
    return
  }

  assert resp.data.controllerServices.component.name.grep(name).size().toInteger() == 1 :
          "Multiple controller services found named '$name'"
  // println prettyPrint(toJson(resp.data))

  def cs = resp.data.controllerServices.component.find { it.name == name }
  assert cs != null

  return cs
}

/**
 Returns a json-backed template structure from NiFi. Null if not found.
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
  // println prettyPrint(toJson(resp.data))

  def t = resp.data.templates.find { it.template.name == name }
  assert t != null

  return t
}

def importTemplate(String templateUri) {
  println "Loading template from URI: $templateUri"
  def templateBody = templateUri.toURL().text

  nifi.request(POST) { request ->
    uri.path = 'process-groups/root/templates/upload'

    requestContentType = 'multipart/form-data'
    MultipartEntity entity = new MultipartEntity()
    entity.addPart("template", new StringBody(templateBody))
    request.entity = entity

    response.success = { resp, xml ->
      switch (resp.statusLine.statusCode) {
        case 200:
          println "[WARN] Template already exists, skipping for now"
//          templateId = "aa11dd74-7898-4ee8-9ee5-c3c04c5985c8"
          // TODO delete template, CS and, maybe a PG
          break
        case 201:
          // grab the trailing UUID part of the location URL header
          templateId = xml.template.id.text()
          println "Template successfully imported into NiFi. ID: $templateId"
          return templateId
        default:
          throw new Exception("Error importing template")
          break
      }
    }
  }
}

def instantiateTemplate(String id) {
  def resp = nifi.post (
          path: 'process-groups/root/template-instance',
          body: [
                  templateId: id,
                  // TODO add slight randomization to the XY to avoid hiding PG behind each other
                  originX: 100,
                  originY: 100
          ],
          requestContentType: JSON
  )

  assert resp.status == 201
}

def loadProcessGroups() {
  println "Loading Process Groups from NiFi"
  def resp = nifi.get(
          path: 'process-groups/root/process-groups'
  )
  assert resp.status == 200
  // println resp.data
  return resp.data.processGroups
}

def findProcessGroup(processGroups, name) {
  processGroups.find { it.component.name == name}
}

/**
 - read the desired pgConfig
 - locate the processor according to the nesting structure in YAML
 (intentionally not using 'search') to pick up a specific PG->Proc
 - update via a partial PUT constructed from the pgConfig
 */
def handleProcessGroup(Map.Entry pgConfig) {
  //println pgConfig

  processGroups = loadProcessGroups()

  if (!pgConfig.value) {
    return
  }

  def pgName = pgConfig.key
  def pg = findProcessGroup(processGroups, pgName)
  assert pg : "Processing Group '$pgName' not found in this instance, check your deployment config?"
  def pgId = pg.id

  // handle the controller services
  pgConfig.value.controllerServices.each {handleControllerService(pgId, it) }

  println "Process Group: $pgConfig.key ($pgId)"
  //println pgConfig

  if (!pg.comments) {
    // update process group comments with a deployment timestamp
    def builder = new JsonBuilder()
    builder {
      revision {
        clientId client
        version pg.revision.version
      }
      id pgId
      component {
        id pgId
        comments defaultComment
      }
    }

    // println builder.toPrettyString()

    resp = nifi.put (
            path: "process-groups/$pgId",
            body: builder.toPrettyString(),
            requestContentType: JSON
    )
    assert resp.status == 200
  }

  // load processors in this group
  resp = nifi.get(path: "process-groups/$pgId/processors")
  assert resp.status == 200

  // construct a quick map of "procName -> [id, fullUri]"
  def processors = resp.data.processors.collectEntries {
    [(it.component.name): [it.id, it.uri, it.component.config.comments]]
  }

  pgConfig.value.processors.each { proc ->
    // check for any duplicate processors in the remote NiFi instance
    def result = processors.findAll { remote -> remote.key == proc.key }
    assert result.entrySet().size() == 1 : "Ambiguous processor name '$proc.key'"

    def procId = processors[proc.key][0]
    def existingComments = processors[proc.key][2]

    println "Stopping Processor '$proc.key' ($procId)"
    stopProcessor(pgId, procId)

    def procProps = proc.value.config.entrySet()

    println "Applying processor configuration"

    resp_get = nifi.get (
            path: "processors/$procId"
    )

    def builder = new JsonBuilder()
    builder {
      revision {
        clientId client
        version resp_get.data.revision.version
      }
      component {
        id procId
        config {
          comments existingComments ?: defaultComment
          properties {
            procProps.each { p ->
              // check if it's a ${referenceToControllerServiceName}
              def ref = p.value =~ /\$\{(.*)}/
              if (ref) {
                def name = ref[0][1] // grab the first capture group (nested inside ArrayList)
                // lookup the CS by name and get the newly generated ID instead of the one in a template
                def newCS = lookupControllerService(name)
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

    // println builder.toPrettyString()

    resp = nifi.put (
            path: "processors/$procId",
            body: builder.toPrettyString(),
            requestContentType: JSON
    )
    assert resp.status == 200

    // check if pgConfig tells us to start this processor
    if (proc.value.state == 'RUNNING') {
      println "Will start it up next"
      startProcessor(pgId, procId)
    } else {
      println "Processor wasn't configured to be running, not starting it up"
    }
  }

  println "Starting Process Group: $pgName ($pgId)"
  startProcessGroup(pgId)
}

def handleControllerService(String pgId, Map.Entry cfg) {
  //println config
  def name = cfg.key
  println "Looking up a controller service '$name'"

  def cs = lookupControllerService(pgId, name)

  println "Found the controller service '$cs.name'. Current state is ${cs.state}."

  if (cs.state == cfg.value.state) {
    println "$cs.name is already in a requested state: '$cs.state'"
    return
  }

  if (cfg.value?.config) {
    println "Applying controller service '$cs.name' configuration"

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


  println "Enabling $cs.name (${cs.id})"
  startControllerService(cs.id)
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


  //println builder.toPrettyString()
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


  int maxWait = 1000 * 30 // up to X seconds
  def resp = nifi.get(path: "process-groups/$pgId")
  assert resp.status == 200
  long start = System.currentTimeMillis()

  // keep polling till active threads shut down, but no more than maxWait time
  while ((System.currentTimeMillis() < (start + maxWait)) &&
          resp.data.runningCount > 0) {
    sleep(1000)
    resp = nifi.get(path: "process-groups/$pgId")
    assert resp.status == 200
    print '.'
  }
  if (resp.data.runningCount == 0) {
    println 'Done'
  } else {
    println "Failed to stop the processing group, request timed out after ${maxWait/1000} seconds"
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
  return
}

def stopControllerService(csId) {
  _changeControllerServiceState(csId, false)
}

def startControllerService(csId) {
  _changeControllerServiceState(csId, true)
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

  // println builder.toPrettyString()

  resp = nifi.put(
          path: "controller-services/$csId",
          body: builder.toPrettyString(),
          requestContentType: JSON
  )
  assert resp.status == 200
}

// script flow below

conf = new Yaml().load(new File(deploymentSpec).text)
assert conf

def nifiHostPort = opts.'nifi-api' ?: conf.nifi.url
if (!nifiHostPort) {
  println 'Please specify a NiFi instance URL in the deployment spec file or via CLI'
  System.exit(-1)
}
nifiHostPort = nifiHostPort.endsWith('/') ? nifiHostPort[0..-2] : nifiHostPort
assert nifiHostPort : "No NiFI REST API endpoint provided"

nifi = new RESTClient("$nifiHostPort/nifi-api/")
nifi.handler.failure = { resp, data ->
  resp.setData(data?.text)
  println "[ERROR] HTTP call failed. Status code: $resp.statusLine: $resp.data"
  // fail gracefully with a more sensible groovy stacktrace
  assert null : "Terminated script execution"
}


client = opts.'client-id' ?: conf.nifi.clientId
assert client : 'Client ID must be provided'

thisHost = InetAddress.localHost
defaultComment = "Last updated by '$client' on ${new Date()} from $thisHost"


handleUndeploy()


def tUri = opts.template ?: conf.nifi.templateUri
assert tUri : "Template URI not provided"
templateId = importTemplate(tUri)
instantiateTemplate(templateId)

println "Configuring Controller Services"

// controller services are dependencies of processors,
// configure them first

println "Configuring Process Groups and Processors"
conf.processGroups.each { handleProcessGroup(it) }

println 'All Done.'
