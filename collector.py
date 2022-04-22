from digitalrig.template import DigitalRig
import argparse
import sys
import traceback
import json
import requests
from datetime import datetime

class CollectorTemplate:
    # assigning  rig_input data to self, so we can use across the functions
    def __init__(self, rig_input):
        self.input_json = rig_input.input_json
        self.rig_url = rig_input.rig_url
        self.tool_details = {}
        self.global_vars = {}
        self.riglet_name = ''
        self.tool_name = ''
    
    def parse_input(self, json_str):
        """
        :param json_str:
        :return:
        """
        return json.loads(json_str)

 

    def getRepoDetails(self):
        url=f"{self.tool_details['url']}/rest/api/1.0/projects/{self.global_vars['key']}/repos/{self.global_vars['repoSlug']}?limit=100"

        headers={
        "Accept": "application/json",
        "Content-Type": "application/json"
        }
        response=requests.get(url,headers=headers,auth=(self.tool_details['username'], self.tool_details['password']))
        print(response.reason)
        if response.ok:
            print('Getting repo details is success')
            print(response.text)
            global_vars = self.global_vars
            global_vars['repoName']=json.loads(response.text)['name']
            global_vars['projectName']=json.loads(response.text)['project']['name']
            # Storing the group id to global_vars to use this at the time of creating subgroup under group where user want to create.
            self.global_vars = global_vars
        else:
            print('Unable to get the repo details')
            print(response.status_code)
            print(response.text)


    def getBranches(self):
        url=f"{self.tool_details['url']}/rest/api/1.0/projects/{self.global_vars['key']}/repos/{self.global_vars['repoSlug']}/branches?start=0&limit=1000"

        headers={
        "Accept": "application/json",
        "Content-Type": "application/json"
        }
        response=requests.get(url,headers=headers,auth=(self.tool_details['username'], self.tool_details['password']))
        print(response.reason)
        if response.ok:
            print('Getting branches list is success')
            branchJson=json.loads(response.text)
            branchSize=branchJson['size']
            print(branchSize)
            i=0
            while(i<branchSize):
                branchName=branchJson['values'][i]['displayId']
                print(branchName)
                lastCommit=branchJson['values'][i]['latestCommit']
                allowed_branch_list = ['development', 'master']
                if branchName in allowed_branch_list:
                    push_to_rabbitMq=self.getCommitDetails(branchName,lastCommit)
                    if push_to_rabbitMq:
                        digitalrig.update_actions_to_rig('success', "Data processing")
                        print('Update actions to rig success')
                        digitalrig.update_timestamp(self.riglet_name, self.tool_name)
                    else:
                        digitalrig.update_actions_to_rig('failure', "Data processing")
                        print('Unable to process the data to rabitMq')
                i+=1
            print(f"{branchSize} branch size")
        else:
            print('Unable to get the branches list')
            print(response.text)
            print(response.status_code)

    def getCommitDetails(self,branchName,lastCommit):
        url=f"{self.tool_details['url']}/rest/api/1.0/projects/{self.global_vars['key']}/repos/{self.global_vars['repoSlug']}/commits?until={lastCommit}&start=0&limit=1000"

        headers={
        "Accept": "application/json",
        "Content-Type": "application/json"
        }
        response=requests.get(url,headers=headers,auth=(self.tool_details['username'], self.tool_details['password']))
        print(response.reason)
        if response.ok:
            commitJson=json.loads(response.text)['values']
            commitsLength=len(commitJson)
            print(commitsLength)
            currentTime = datetime.now()
            currentTime=currentTime.strftime('%Y-%m-%d %H:%M:%S')
            l=0
            finalMessage="["
            while l<commitsLength:
                commitTimeStamp=commitJson[l]['committerTimestamp']
                self.runtime=None
                if self.runtime==None:
                    self.global_vars['lob']='bhar'
                    self.global_vars['product_group']='stu'
                    self.global_vars['application_name']= 'ppl'
                    self.global_vars['org']= 'andi'
                    self.global_vars['team']= 'sin'
                    committer_name=commitJson[l]['committer']['name']
                    committer_email=commitJson[l]['committer']['emailAddress']
                    committed_date=commitJson[l]['committerTimestamp']
                    commit_id=commitJson[l]['id']
                    message="{ \"lob\": \""+self.global_vars['lob']+"\", \"application_name\": \""+self.global_vars['product_group']+"\", \"team_name\": \""+self.global_vars['application_name']+"\", \"org_name\": \""+self.global_vars['org']+"\", \"repo_name\": \""+self.global_vars['repoName']+"\", \"project_name\": \""+self.global_vars['projectName']+"\", \"branch_name\": \""+branchName+"\", \"time\": \""+str(currentTime)+"\", \"committer_name\": \""+committer_name+"\", \"committer_email\": \""+committer_email+"\", \"committed_date\": \""+str(committed_date)+"\", \"commit_id\": \""+str(commit_id)+"\", \"riglet_name\": \""+self.global_vars['team']+"\", \"repo_id\": \""+self.global_vars['repoSlug']+"\" }, "
                    finalMessage=finalMessage+message
                elif self.runtime<commitTimeStamp:
                    committer_name=commitJson[l]['committer']['name']
                    committer_email=commitJson[l]['committer']['emailAddress']
                    committed_date=commitJson[l]['committerTimestamp']
                    commit_id=commitJson[l]['id']
                    message="{ \"lob\": \""+self.global_vars['lob']+"\", \"application_name\": \"xyz\", \"team_name\": \""+self.global_vars['team']+"\", \"repo_name\": \""+self.global_vars['repoName']+"\", \"project_name\": \""+self.global_vars['projectName']+"\", \"branch_name\": \""+branchName+"\", \"time\": \""+str(currentTime)+"\", \"committer_name\": \""+committer_name+"\", \"committer_email\": \""+committer_email+"\", \"committed_date\": \""+str(committed_date)+"\", \"commit_id\": \""+str(commit_id)+"\", \"riglet_name\": \""+self.riglet_name+"\", \"repo_id\": \""+self.global_vars['repoSlug']+"\" }, "
                    finalMessage=finalMessage+message
                else:
                    break

                l+=1
            finalMessage=finalMessage[:-2:]
            finalMessage=finalMessage+']'
            print(finalMessage)
            if len(finalMessage)>2:
                result=digitalrig.push_to_rabbitMq('bitbucket', 'bitbucket_raw_rig', finalMessage)
                print('Data collection for '+branchName+' is successfull')
                digitalrig.update_actions_to_rig('success', f"Data Collection - {branchName}")
            else:
                print('There is no new data to collect for the branch '+branchName+'')
                print('Data collection for '+branchName+' is failure')
                digitalrig.update_actions_to_rig('failure', f"Data Collection - {branchName}")
            print(result)
            return result
        else:
            print('Unable to get commit details')
            print('Data collection for '+branchName+' is failure')
            digitalrig.update_actions_to_rig('failure', f"Data Collection - {branchName}")
            print(response.text)
            print(response.status_code)
            return False


    # used to trigger the actions based on the user's input
    def trigger(self):
        json_dict = self.parse_input(self.input_json)
        riglet_name = json_dict['team_info']['name']
        tool_name = json_dict['scm']['tool']['name']
        projects = json_dict['scm']['project']
        self.riglet_name=riglet_name
        self.tool_name=tool_name
        response_dict=digitalrig.get_tool_details(riglet_name, tool_name)
        self.tool_details = json.loads(response_dict)
        for project in projects:
            if 'key' in project and 'repoSlug' in project:
                print(project['key'])
                print(project['repoSlug'])
                global_vars = self.global_vars
                global_vars['key']=project['key'].strip()
                global_vars['repoSlug']=project['repoSlug'].strip()
                # Storing the group id to global_vars to use this at the time of creating subgroup under group where user want to create.
                self.global_vars = global_vars
                # response_lastruntime=digitalrig.last_run_time(riglet_name, tool_name)
                # if response_lastruntime!=False:
                #     value=json.loads(response_lastruntime)
                #     print(value)
                #     self.global_vars['lob'] = value[0]['lobName']
                #     self.global_vars['team'] = value[0]['teamName']
                #     self.global_vars['org']= value[0]['orgName']
                #     self.global_vars['product_group'] = value[0]['productGroupName']
                #     self.global_vars['application_name']= value[0]['applicationName']
                #     if value[0]== None:
                #         self.runtime=None
                #     else:
                #         collectorStatus=value[0]['collectorStatus']
                #         if len(collectorStatus)==0:
                #             self.runtime=None
                #         else:
                #             size=0
                #             while size<len(collectorStatus):
                #                 if collectorStatus[size]['tool']==tool_name:                    
                #                     if 'lastRunTime' in collectorStatus[size].keys():
                #                         print('YES KEY IS PRESENT')
                #                         self.runtime=collectorStatus[size]['lastRunTime']
                #                         break
                #                     else:
                #                         print('Because of running the riglet for the first time, there is no lastRunTime')
                #                         self.runtime=None
                #                         print(self.runtime)
                #                 else:
                #                     print(f"Tool name - {tool_name} is not found in the tiglet - {riglet_name} ")
                #                     self.runtime=None
                #                     print(self.runtime)
                #                 size+=1
                self.getRepoDetails()
                self.getBranches()
            else:
                print('To collect the metrics user should provide project key and repo slug')
            




if __name__ == '__main__':
    try:
        PARSER = argparse.ArgumentParser(description='DigitalRig connector argument list.')
        PARSER.add_argument('rig_url', help='Provide a rig url to connect')
        PARSER.add_argument('input_json', help='Provide riglet json input')

        INPUT_ARGS = PARSER.parse_args()
        COLLECTOR_TEMPLATE = CollectorTemplate(INPUT_ARGS)
        devops_tool=json.loads(INPUT_ARGS.input_json)['scm']['tool']['name']
        digitalrig=DigitalRig(INPUT_ARGS,devops_tool)
        try:           
            COLLECTOR_TEMPLATE.trigger()
        except requests.exceptions.SSLError as e:
            print(e)
            print('ssl error')
        except requests.exceptions.ConnectionError as timeout_error:
            print(timeout_error)
            print('Failed to establish connection. Connection timed out.')
        except TypeError as type_err:
            print(type_err)
            traceback.print_exc()
            print('User trying to iterate the null elements,please check the inputs')
        except KeyError as key_error:
            traceback.print_exc()
            print(key_error)
            print(f'value for {key_error} is missing, so we cannot perform actions. So please recheck the inputs')
    except Exception as ex:
        traceback.print_exc()
        sys.exit(1)


        # python bitbucket_collector.py http://run.mocky.io "{\"riglet_info\":{\"name\":\"DIGITALRIG\"},\"scm\":{\"tool\":{\"name\":\"bitbucket\"},\"project\":[{\"key\":\"DRI\",\"repoSlug\":\"digitalrigv2\"}]}}"