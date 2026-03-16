#!/usr/bin/python

# Copyright: (c) 2019, Johnathan Kupferer <jkupfere@redhat.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from datetime import datetime, timedelta
import os
import re
import time
import requests

from ansible.plugins.action import ActionBase
from ansible.utils.display import Display

display = Display()

RETRY_COUNT = 3
RETRY_DELAY = 5
RETRY_BACKOFF = 2

datetime_re = re.compile(r'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$')

def parse_time_interval(interval):
    if isinstance(interval, int):
        return timedelta(seconds=interval)
    if isinstance(interval, str) \
    and interval != '':
        m = re.match(r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s?)?$', interval)
        if m:
            return timedelta(
                days=int(m.group(1) or 0),
                hours=int(m.group(2) or 0),
                minutes=int(m.group(3) or 0),
                seconds=int(m.group(4) or 0)
            )
        else:
            return None
    return None

class ActionModule(ActionBase):
    def run(self, tmp=None, task_vars=None, **_):
        result = super(ActionModule, self).run(tmp, task_vars)
        module_args = self._task.args.copy()
        anarchy_subject_name = task_vars['anarchy_subject_name']
        anarchy_url = task_vars['anarchy_url']
        anarchy_run_pod_name = task_vars['anarchy_run_pod_name']
        anarchy_runner_name = task_vars['anarchy_runner_name']
        anarchy_runner_token = task_vars['anarchy_runner_token']

        action = module_args.get('action', None)
        after = module_args.get('after', None)
        cancel = module_args.get('cancel', [])
        vars = module_args.get('vars', {})

        if isinstance(after, datetime):
            after = after.strftime('%FT%TZ')
        elif not after:
            after = datetime.utcnow().strftime('%FT%TZ')
        elif datetime_re.match(after):
            pass
        else:
            interval = parse_time_interval(after)
            if interval:
                after = (datetime.utcnow() + interval).strftime('%FT%TZ')
            else:
                result['failed'] = True
                result['message'] = 'Invalid value for `after`: {}'.format(after)
                return result

        url = anarchy_url + '/run/subject/' + anarchy_subject_name + '/actions'
        headers = {'Authorization': 'Bearer {}:{}:{}'.format(
            anarchy_runner_name, anarchy_run_pod_name, anarchy_runner_token
        )}
        payload = dict(action=action, after=after, cancel=cancel, vars=vars)

        last_exception = None
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                response = requests.post(url, headers=headers, json=payload)
                response.raise_for_status()
                response_json = response.json()
                result['action'] = response_json['result']
                result['failed'] = not response_json['success']
                return result
            except (requests.exceptions.RequestException, ValueError, KeyError) as e:
                last_exception = e
                display.warning(
                    "anarchy_schedule_action {}: attempt {}/{} failed: {}".format(
                        anarchy_subject_name, attempt, RETRY_COUNT, str(e)
                    )
                )
                if attempt < RETRY_COUNT:
                    time.sleep(RETRY_DELAY * (RETRY_BACKOFF ** (attempt - 1)))

        result['failed'] = True
        result['msg'] = "anarchy_schedule_action failed after {} attempts: {}".format(
            RETRY_COUNT, str(last_exception)
        )
        return result
