"""
millennium_query.py

Query the Millennium Simulation UWS/TAP client.

Similar to tapquery in gavo.votable

GNU General Public License v3 (GNU GPLv3)
You should have received a copy of the GNU General Public License
along with this software.  If not, see <http://www.gnu.org/licenses/>.
You may use, modify, and/or redistribute this software under the
terms of the GNU General Public License version 3 or any future version.

Copyright(C) 2016 by
Trey Wenger; tvwenger@gmail.com

11 Mar 2016 - TVW Finalized version 1.0
"""
_PROG_NAME = 'millennium_query.py'
_VERSION = 'v1.0'

import os
import time
import requests
from lxml import etree

_MILL_URL = "http://galformod.mpa-garching.mpg.de/millenniumtap/async"

def get_cookies(username,password):
    """
    Connect to the host just to get the login cookies
    """
    session = requests.Session()
    session.auth = (username,password)
    response = session.get(_MILL_URL)
    if "Login at Millennium TAP" in response.content:
        raise RuntimeError("Login credentials not valid for MillenniumTAP")
    cookies = session.cookies
    session.close()
    return cookies

class MillenniumQuery:
    """
    Container for handling query to Millennium Simulation database
    """
    def __init__(self,username,password,job_id=None,
                 query_lang='SQL',results_format='csv',
                 maxrec=100000,cookies=None):
        """
        Set up shared variables for this container
        """
        self.url = _MILL_URL
        self.post_data = {'LANG':query_lang,
                          'FORMAT':results_format,
                          'MAXREC':maxrec,
                          'REQUEST':'doQuery',
                          'VERSION':'1.0'}
        self.job_id = job_id
        self.job_url = None
        # If we already know the Job ID, set up proper URL
        if self.job_id is not None:
            self.job_url = '{0}/{1}'.format(self.url,self.job_id)
        # Get cookies if we don't already have some
        if self.cookies is None:
            self.cookies = get_cookies(username,password)
        else:
            self.cookies = cookies
        # Set up session
        self.session = requests.Session()
        self.session.auth = (username,password)

    def query(self,query):
        """
        Send query to server
        """
        # If we already know the job ID, use that URL
        if self.job_url is not None:
            url = self.job_url
        else:
            url = self.url
        post_data = self.post_data.copy()
        post_data['QUERY'] = query
        # Set up job
        response = self.session.post(url,data=post_data,
                                     cookies=self.cookies)
        if "Login at Millennium TAP" in response.content:
            raise RuntimeError("Login credentials not valid for MillenniumTAP")
        # Save cookies in case we don't already have some
        self.cookies = self.session.cookies
        # If we don't already have one, get Job ID from output XML
        if self.job_id is None:
            root = etree.fromstring(response.content)
            self.job_id = root.find('uws:jobId',root.nsmap).text
            self.job_url = '{0}/{1}'.format(self.url,self.job_id)

    def run(self):
        """
        Start job on server
        """
        # Check that we've already run query()
        if self.job_url is None:
            raise RuntimeError("Must first call query() to set up MillenniumQuery")
        url = '{0}/{1}'.format(self.job_url,'phase')
        post_data = self.post_data.copy()
        post_data['PHASE'] = 'RUN'
        response = self.session.post(url,data=post_data,
                                     cookies=self.cookies)
        if "Login at Millennium TAP" in response.content:
            raise RuntimeError("Login credentials not valid for MillenniumTAP")

    def wait(self,max_attempts=100):
        """
        Wait for job to finish, then get the results
        Throw error if waiting for too long
        """
        niter = 0
        while True:
            phase = self.phase
            if phase == 'COMPLETED':
                break
            elif phase == 'ABORTED':
                raise RuntimeError("MillenniumQuery job {0} aborted!".format(self.job_id))
            elif phase == 'ERROR':
                raise RuntimeError("{0}".format(self.error_message))
            if max_attempts:
                if niter > max_attempts:
                    raise RuntimeError("Did not get MillenniumQuery results after {0} iterations, job {1}".format(max_attempts,self.job_id))
            niter += 1
            # wait for 120 seconds or niter*2^(1/4) seconds, whichever
            # is shorter
            wait_time = 1.189207115002721*increment
            time.sleep(min(120.,wait_time))

    def save(self,filename):
        """
        Save results to filename
        """
        if self.job_url is None:
            raise RuntimeError("Must first call query() to set up MillenniumQuery")
        if self.phase != 'COMPLETED':
            raise RuntimeError("Job {0} is not completed yet!".format(self.job_id))
        url = '{0}/results/result'.format(self.job_url)
        # Stream chunks from download since the file could be huge
        response = self.session.get(url,cookies=self.cookies,stream=True)
        if "Login at Millennium TAP" in response.content:
            raise RuntimeError("Login credentials not valid for MillenniumTAP")
        with open(filename,'w') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk: # filter out "keep-alive" chunks
                    f.write(chunk)

    def close(self):
        """
        Close the session
        """
        self.session.close()
        
    @property
    def phase(self):
        """
        Returns the current phase of the job
        """
        if self.job_url is None:
            raise RuntimeError("Must first call query() to set up MillenniumQuery")
        response = self.session.get(self.job_url,cookies=self.cookies)
        if "Login at Millennium TAP" in response.content:
            raise RuntimeError("Login credentials not valid for MillenniumTAP")
        root = etree.fromstring(response.content)
        phase = root.find('uws:phase',root.nsmap).text
        return phase

    @property
    def error_message(self):
        """
        Returns the error message for this job
        """
        if self.job_url is None:
            raise RuntimeError("Must first call query() to set up MillenniumQuery")
        response = self.session.get(self.job_url,cookies=self.cookies)
        if "Login at Millennium TAP" in response.content:
            raise RuntimeError("Login credentials not valid for MillenniumTAP")
        root = etree.fromstring(response.content)
        message = root.find('uws:errorSummary/uws:message',root.nsmap).text
        return message
