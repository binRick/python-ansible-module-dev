#!/usr/bin/python
import json, sys, shlex, datetime, os, re, subprocess, shutil, time, logging, functools
#from tai64n import decode_tai64n


ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}



DOCUMENTATION = '''
---
module: manage_services

short_description: This is my sample module

version_added: "2.4"

description:
    - "This is my longer description explaining my sample module"

options:
    services:
        description:
            - Services to configure
        required: true
    name:
        description:
            - This is the message to send to the sample module
        required: true
    new:
        description:
            - Control to demo if the result of this module is changed or not
        required: false

extends_documentation_fragment:
    - azure

author:
    - Your Name (@yourhandle)
'''

EXAMPLES = '''
# Pass in a message
- name: Test with a message
  manage_services:
    name: hello world

# pass in a message and have changed true
- name: Test with a message and changed output
  manage_services:
    name: hello world
    new: true

# fail the module
- name: Test failure of the module
  manage_services:
    name: fail me
'''

RETURN = '''
original_message:
    description: The original name param that was passed in
    type: str
    returned: always
message:
    description: The output message that the sample module generates
    type: str
    returned: always
'''



from ansible.module_utils.basic import AnsibleModule


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return int(calendar.timegm(obj.timetuple()))
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


def json_decoder(obj):
    for key, val in obj.items():
        if key in ('_id', 'user', 'org', 'cust'):
            if val is not None:
                val = ObjectId(val)
        elif key in ('users',):
            if isinstance(val, list):
                val = [ObjectId(k) for k in val]
        elif key in ('created', 'modified', 'last_login',):
            if isinstance(val, (float, int)):
                val = datetime.utcfromtimestamp(val)

        obj[key] = val

    return obj

LOG_DIR = '/var/log/service'
SV_SCRIPT = """#!/bin/sh
exec 2>&1
%(extra)s
exec setuidgid %(user)s %(cmd)s"""
LOG_SCRIPT = """#!/bin/sh
exec setuidgid root multilog t s%(max_size)s ./main"""
RE_PID = re.compile(r'\(pid\s+(\d+)\)')


class JSONSerializer(object):

    def encode(self, obj, fd=None):
        try:
            if fd:
                return json.dump(obj, fd, cls=JSONEncoder)
            else:
                return json.dumps(obj, cls=JSONEncoder)
        except (TypeError, ValueError), e:
            raise Exception(str(e))

    def decode(self, msg=None, fd=None):
        try:
            if msg:
                return json.loads(msg, object_hook=json_decoder)
            elif fd:
                return json.load(fd, object_hook=json_decoder)
        except (TypeError, ValueError), e:
            raise Exception(str(e))


def serialize(obj):
    return JSONSerializer().encode(obj)






class ServiceError(Exception): pass
class ProcessError(Exception): pass




def nested_set(dictionary, keys, value):
    keys = keys.split('.')
    for key in keys[:-1]:
        dictionary = dictionary.setdefault(key, {})
    dictionary[keys[-1]] = value

def nested_get(dictionary, dotted_key):
    keys = dotted_key.split('.')
    return functools.reduce(lambda d, key: d.get(key) if d else None, keys, dictionary)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def getTimestampMilliseconds():
    return int(time.time() * 1000)

def getTimestamp():
    return int(time.time())



def run_module():
    module_args = dict(
        services=dict(type='dict', required=True),
        service_dir=dict(type='str', required=True),
        sv_dir=dict(type='str', required=True),
        name=dict(type='str', required=True),
        new=dict(type='bool', required=False, default=False)
    )
    result = dict(
        changed=False,
        original_message='',
        message=''
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )
    SV_DIR = module.params['sv_dir']
    SERVICE_DIR = module.params['service_dir']
    if module.check_mode:
        module.exit_json(**result)
    result['original_message'] = module.params['name']
    result['message'] = 'goodbye'
    if module.params['new']:
        result['changed'] = True
    if module.params['name'] == 'fail me':
        module.fail_json(msg='You requested this to fail', **result)

    def getRunFileContents(**kwargs):
        return SV_SCRIPT % {
          'cmd': kwargs['cmd'],
          'user': kwargs.get('user', 'root'),
          'extra': kwargs.get('extra', ''),
        }


    def add(name, script=None, max_log_size=100000, **kwargs):
        '''Add a service and supervise it.
        :param script: script content
        :param max_log_size: maximum log size
        :param kwargs: extra parameters:
            - cmd: command to execute
            - user: user under wich the service must run
            - extra: extra script content executed before the command
        '''
        name = re.sub(r'\W+', '_', name)

        sv_dir = _get_sv_dir(name)
        if not os.path.exists(sv_dir):
            makedirs(sv_dir)

        # Create service run script
        if not script:
            if not kwargs.get('cmd'):
                raise ServiceError('missing script or cmd parameters')

            script = getRunFileContents(kwargs)
            """
            script = SV_SCRIPT % {
                    'cmd': kwargs['cmd'],
                    'user': kwargs.get('user', 'root'),
                    'extra': kwargs.get('extra', ''),
                    }
            """

        sv_script = _get_sv_script(name)
        with open(sv_script, 'wb') as fd:
            fd.write(script)

        _set_log(name)

        # Create log run script
        svlog_script = _get_svlog_script(name)
        with open(svlog_script, 'wb') as fd:
            fd.write(LOG_SCRIPT % {'max_size': max_log_size})

        _set_scripts(name)

    def _list():
        '''Get the list of supervised services.
        '''
        if not os.path.exists(SV_DIR):
            return []
        return os.listdir(SV_DIR)

    def get(name):
        '''Get the service run script.
        '''
        file_script = _get_sv_script(name)
        if not os.path.exists(file_script):
            return ''

        with open(file_script) as fd:
            res = fd.read()
        return res

    def get_pid(name):
        '''Get the supervised process pid.
        '''
        stdout, stderr, return_code = _popen(['svstat', _get_sv_dir(name)])
        res = RE_PID.search(stdout)
        return int(res.group(1)) if res else None

    def get_log(name, include_time=True):
        '''Get the service log.
        '''
        log_file = os.path.join(_get_svlog_dir(name), 'main/current')
        if not os.path.exists(log_file):
            return ''

        with open(log_file) as fd:
            data = fd.read()

        res = []
        for line in reversed(data.splitlines()):
            if line:
                ts, log = line.split(' ', 1)
                if include_time:
                    if line.startswith('@'):
                        log = '%s %s' % (decode_tai64n(ts.lstrip('@')).strftime('%Y-%m-%d %H:%M:%S'), log)
                    else:
                        log = line
                res.append(log)
        return '\n'.join(res)

    def update(name, script):
        '''Update the service run script.
        '''
        up = get_pid(name) is not None
        if up:
            stop(name)
        with open(_get_sv_script(name), 'wb') as fd:
            fd.write(script)
        if up:
            start(name)

    def remove(name, remove_log=True):
        '''Remove a service.
        '''
        exit(name)
        _wait_stopped(name)

        sv_symlink = _get_service_symlink(name)
        if os.path.exists(sv_symlink):
            try:
                os.remove(sv_symlink)
            except Exception, e:
                raise ServiceError(str(e))

        sv_dir = _get_sv_dir(name)
        if os.path.exists(sv_dir):
            rmtree(sv_dir)

        if remove_log:
            log_dir = os.path.join(LOG_DIR, name)
            if os.path.exists(log_dir):
                rmtree(log_dir)

    def validate(name):
        try:
            _set_log(name)
            _set_scripts(name)
            return True
        except ServiceError:
            return False

    def start(name):
        '''Start the service.
        '''
        if not validate(name):
            return False
        return _svc_exec(name, '-u')

    def stop(name):
        '''Stop the service.
        '''
        return _svc_exec(name, '-+d')

    def exit(name):
        '''Stop the service and the supervise process.
        '''
        return _svc_exec(name, '-+dx')

    def kill(name):
        '''Kill the service process.
        '''
        return _svc_exec(name, '-+k')

    def _set_log(name):
        '''Create log directory and service log symlink.
        '''
        log_dir = os.path.join(LOG_DIR, name)
        if not os.path.exists(log_dir):
            makedirs(log_dir)

        svlog_dir = _get_svlog_dir(name)
        if not os.path.exists(svlog_dir):
            makedirs(svlog_dir)

        log_symlink = os.path.join(svlog_dir, 'main')
        if not os.path.exists(log_symlink):
            os.symlink(log_dir, log_symlink)

    def _set_scripts(name):
        for file in (_get_sv_script(name), _get_svlog_script(name)):
            try:
                os.chmod(file, 0755)
            except OSError, e:
                raise ServiceError('failed to update %s permissions: %s' % (file, str(e)))

        # Enable service
        sv_symlink = _get_service_symlink(name)
        if not os.path.exists(sv_symlink):
            os.symlink(_get_sv_dir(name), sv_symlink)

    def _wait_stopped(name):
        '''Wait for the service to stop to avoid supervise error messages.
        '''
        for i in range(10):
            if not get_pid(name):
                return True
            time.sleep(.5)

    def _svc_exec(name, arg):
        cmd = ['svc', arg, _get_sv_dir(name), _get_svlog_dir(name)]
        return _popen(cmd)[2] == 0

    def _popen(cmd):
        stdout, stderr, return_code = None, None, None
        proc = subprocess.Popen(cmd,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = proc.communicate()
            return_code = proc.returncode
        except Exception, e:
            raise ProcessError(str(e))
        return stdout, stderr, return_code

    def makedirs(dir):
        try:
            os.makedirs(dir)
        except OSError, e:
            raise ServiceError(str(e))

    def rmtree(dir):
        try:
            shutil.rmtree(dir)
        except OSError, e:
            raise ServiceError(str(e))

    def _get_sv_dir(name):
        return os.path.join(SV_DIR, name)

    def _get_svlog_dir(name):
        return os.path.join(_get_sv_dir(name), 'log')

    def _get_sv_script(name):
        return os.path.join(_get_sv_dir(name), 'run')

    def _get_svlog_script(name):
        return os.path.join(_get_svlog_dir(name), 'run')

    def _get_service_symlink(name):
        return os.path.join(SERVICE_DIR, name)

    def listServices():
        items = []
        for name in sorted(_list()):
            items.append({
                    'name': name,
                    'script': get(name),
                    'status': get_pid(name) is not None,
                    })
        return serialize({'result': items})






    result['setup'] = {
      'paths': {
        'svc': module.get_bin_path('svc', True),
        'supervise': module.get_bin_path('supervise', True),
        'tai64nlocal': module.get_bin_path('tai64nlocal', True),
        'svstat': module.get_bin_path('svstat', True),
      },
    }
    result['XX'] = module.run_command([result['setup']['paths']['svstat'], '/service/OpenVPN_tcp'])
    _STATE_HANDLERS = {
      'start': ['started','running','start', True],
      'stop': ['stopped','stop','exit','exited'],
      'kill': ['restarted','restart','kill','killed'],
      'remove': ['absent','no',False,'remove','removed','False','false','deleted','destroyed','nuked'],
    }
    def getServices():
        s=[]
        for _s in json.loads(listServices())['result']:
            s.append(_s['name'])
        return s
    class ServiceStateHandler():
        def __init__(self):
            self.preServices = getServices()
            self.started = getTimestampMilliseconds()
            self.services = json.loads(json.dumps(self.preServices))
            self.servicesStarted = []
            self.servicesStartedDuration = 0

        def requestServices(self, requestedServices):
            self.requestedServices = requestedServices

        def getServicesByStates(self, states):
            s=[]
            for _s in self.requestedServices.keys():
              for state in states:
                for k in _STATE_HANDLERS[state]:
                    if k == self.requestedServices[_s]['state']:
                        s.append(_s)
            return s
        def getServicesWithDifferentLogRunFile(self):
            s=[]
            return s
        def getServicesWithDifferentRunFile(self):
            s=[]
            return s
            
        def getServicesToStop(self):
            s=[]
            return s
        def getServicesToUpdateRunFile(self):
            s=[]
            return s
        def getServicesToUpdateLogRunFile(self):
            s=[]
            return s
        def getServicesToCreate(self):
            s=[]
            return s
        def findforkedRogueProcesses(self):
            s=[]
            return s
        def getRogueServices(self):
            s=[]
            return s
        def createServices(self):
            s=[]
            s.append('123')
            return s
        def summary(self):
            r = {
              'started': self.started,
              'qty': len(self.services),
              'requestedServices': self.requestedServices,
              'service_dir': SERVICE_DIR,
              'pre': self.preServices,
            }
            r['servicesTo'] = {
                  'stop': self.getServicesByStates(['stop']),
                  'create': self.getServicesByStates(['start','stop','kill']),
                  'updateRun': self.getServicesWithDifferentRunFile(),
                  'updateLogRun': self.getServicesWithDifferentRunFile(),
                  'start': self.getServicesByStates(['start']),
                  'remove': self.getServicesByStates(['remove']),
            }
            r['serviceResults'] =  {
                  'created': self.createServices(),
            }
            r['serviceUpdates'] = {
                  'started': self.servicesStarted,
            }
            r['serviceDurations'] = {
                  'started': str(self.servicesStartedDuration)+'ms',
            }
            r['forkedRogueProcessFinderResult'] = self.findforkedRogueProcesses() 
            r['rogueServices'] = self.getRogueServices() 
            r['post'] = getServices() 
            r['ended'] = getTimestampMilliseconds() 
            r['duration'] = r['ended'] - r['started']
            r['duration_human'] = str(r['duration'])+'ms'
            return r

    def getServicePids(services):
        p={}
        for _s in services:
            p[_s] = get_pid(_s)
        return p
    def ensureServiceStates(services):
        for _s in services:
            
            p[_s] = get_pid(_s)
        return True

    SSH = ServiceStateHandler()
    module.params['service_dir']
    result['loadRequestedServices'] = SSH.requestServices(module.params['services'])
    result['sshSummary'] = SSH.summary()



#    result['initialServices'] = getServices()
#    result['pids'] = getServicePids(result['initialServices'])
    
#    result['currentServices'] = getServices()
    module.exit_json(**result)
















def main():
    run_module()

if __name__ == '__main__':
    main()

