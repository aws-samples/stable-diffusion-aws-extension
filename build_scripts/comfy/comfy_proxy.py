import concurrent.futures
import re
import signal
import threading

import boto3
import requests
from aiohttp import web

import folder_paths
import server
from execution import PromptExecutor
import execution
import comfy

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
from dotenv import load_dotenv

import fcntl
import hashlib

import base64
import datetime
import json
import logging
import os
import sys
import tarfile
import time
import uuid
import gc
from dataclasses import dataclass
from typing import Optional

from boto3.dynamodb.conditions import Key

DISABLE_AWS_PROXY = 'DISABLE_AWS_PROXY'
sync_msg_list = []
client_release_map = {}
lock_status = False
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

is_on_sagemaker = os.getenv('ON_SAGEMAKER') == 'true'
is_on_ec2 = os.getenv('ON_EC2') == 'true'

if is_on_ec2:
    env_path = '/etc/environment'

    if 'ENV_FILE_PATH' in os.environ and os.environ.get('ENV_FILE_PATH'):
        env_path = os.environ.get('ENV_FILE_PATH')

    load_dotenv('/etc/environment')
    logger.info(f"env_path{env_path}")

    env_keys = ['ENV_FILE_PATH', 'COMFY_INPUT_PATH', 'COMFY_MODEL_PATH', 'COMFY_NODE_PATH', 'COMFY_API_URL',
                'COMFY_API_TOKEN', 'COMFY_ENDPOINT', 'COMFY_NEED_SYNC', 'COMFY_NEED_PREPARE', 'COMFY_BUCKET_NAME',
                'MAX_WAIT_TIME', 'MSG_MAX_WAIT_TIME', 'THREAD_MAX_WAIT_TIME', DISABLE_AWS_PROXY, 'DISABLE_AUTO_SYNC']

    for item in os.environ.keys():
        if item in env_keys:
            logger.info(f'evn key： {item} {os.environ.get(item)}')

    DIR3 = f"/container/workflows/{os.getenv('WORKFLOW_NAME')}/ComfyUI/input"
    DIR1 = f"/container/workflows/{os.getenv('WORKFLOW_NAME')}/ComfyUI/models"
    DIR2 = f"/container/workflows/{os.getenv('WORKFLOW_NAME')}/ComfyUI/custom_nodes"

    if 'COMFY_INPUT_PATH' in os.environ and os.environ.get('COMFY_INPUT_PATH'):
        DIR3 = os.environ.get('COMFY_INPUT_PATH')
    if 'COMFY_MODEL_PATH' in os.environ and os.environ.get('COMFY_MODEL_PATH'):
        DIR1 = os.environ.get('COMFY_MODEL_PATH')
    if 'COMFY_NODE_PATH' in os.environ and os.environ.get('COMFY_NODE_PATH'):
        DIR2 = os.environ.get('COMFY_NODE_PATH')

    api_url = os.environ.get('COMFY_API_URL')
    api_token = os.environ.get('COMFY_API_TOKEN')
    comfy_need_sync = os.environ.get('COMFY_NEED_SYNC', True)
    comfy_need_prepare = os.environ.get('COMFY_NEED_PREPARE', False)
    bucket_name = os.environ.get('COMFY_BUCKET_NAME')
    thread_max_wait_time = os.environ.get('THREAD_MAX_WAIT_TIME', 60)
    max_wait_time = os.environ.get('MAX_WAIT_TIME', 86400)
    msg_max_wait_time = os.environ.get('MSG_MAX_WAIT_TIME', 86400)
    is_master_process = os.getenv('MASTER_PROCESS') == 'true'
    program_name = os.getenv('PROGRAM_NAME')
    no_need_sync_files = ['.autosave', '.cache', '.autosave1', '~', '.swp']

    need_resend_msg_result = []
    PREPARE_ID = 'default'
    # additional
    PREPARE_MODE = 'additional'

    if not api_url:
        raise ValueError("API_URL environment variables must be set.")

    if not api_token:
        raise ValueError("API_TOKEN environment variables must be set.")

    headers = {"x-api-key": api_token, "Content-Type": "application/json", "username": "api"}

    global_thread_pool = concurrent.futures.ThreadPoolExecutor()


    def send_msg_to_all_sockets(event: str, msg: dict):
        sockets = server.PromptServer.instance.sockets
        for socket in sockets.keys():
            client_id = socket
            server.PromptServer.instance.loop.call_soon_threadsafe(
                server.PromptServer.instance.messages.put_nowait, (event, msg, client_id))

    def get_endpoint_name_by_workflow_name(name: str, endpoint_type: str = 'async'):
        return f"comfy-{endpoint_type}-{name}"


    def save_images_locally(response_json, local_folder):
        try:
            data = response_json.get("data", {})
            prompt_id = data.get("prompt_id")
            image_video_data = data.get("image_video_data", {})

            if not prompt_id or not image_video_data:
                logger.info("Missing prompt_id or image_video_data in the response.")
                return

            folder_path = os.path.join(local_folder, prompt_id)
            os.makedirs(folder_path, exist_ok=True)

            for image_name, image_url in image_video_data.items():
                image_response = requests.get(image_url)
                if image_response.status_code == 200:
                    image_path = os.path.join(folder_path, image_name)
                    with open(image_path, "wb") as image_file:
                        image_file.write(image_response.content)
                    logger.info(f"Image '{image_name}' saved to {image_path}")
                else:
                    logger.info(
                        f"Failed to download image '{image_name}' from {image_url}. Status code: {image_response.status_code}")

        except Exception as e:
            logger.info(f"Error saving images locally: {e}")


    def calculate_file_hash(file_path):
        # 创建一个哈希对象
        hasher = hashlib.sha256()
        # 打开文件并逐块更新哈希对象
        with open(file_path, 'rb') as file:
            buffer = file.read(65536)  # 64KB 的缓冲区大小
            while len(buffer) > 0:
                hasher.update(buffer)
                buffer = file.read(65536)
        # 返回哈希值的十六进制表示
        return hasher.hexdigest()


    def save_files(prefix, execute, key, target_dir, need_prefix):
        if key in execute['data']:
            temp_files = execute['data'][key]
            for url in temp_files:
                loca_file = get_file_name(url)
                response = requests.get(url)
                # if target_dir not exists, create it
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                logger.info(f"Saving file {loca_file} to {target_dir}")
                if loca_file.endswith("output_images_will_be_put_here"):
                    continue
                if need_prefix:
                    with open(f"{target_dir}/{prefix}_{loca_file}", 'wb') as f:
                        f.write(response.content)
                    # current override exist
                    with open(f"{target_dir}/{loca_file}", 'wb') as f:
                        f.write(response.content)
                else:
                    with open(f"{target_dir}/{loca_file}", 'wb') as f:
                        f.write(response.content)


    def get_file_name(url: str):
        file_name = url.split('/')[-1]
        file_name = file_name.split('?')[0]
        return file_name


    def send_service_msg(server_use, msg):
        event = msg.get('event')
        data = msg.get('data')
        sid = msg.get('sid') if 'sid' in msg else None
        server_use.send_sync(event, data, sid)


    def handle_sync_messages(server_use, msg_array):
        already_synced = False
        global sync_msg_list
        for msg in msg_array:
            for item_msg in msg:
                event = item_msg.get('event')
                data = item_msg.get('data')
                sid = item_msg.get('sid') if 'sid' in item_msg else None
                if data in sync_msg_list:
                    continue
                sync_msg_list.append(data)
                if event == 'finish':
                    already_synced = True
                elif event == 'executed':
                    global need_resend_msg_result
                    need_resend_msg_result.append(msg)
                server_use.send_sync(event, data, sid)

        return already_synced


    def execute_proxy(func):
        def wrapper(*args, **kwargs):

            def send_error_msg(executor, prompt_id, msg):
                mes = {
                    "prompt_id": prompt_id,
                    "node_id": "",
                    "node_type": "on cloud",
                    "executed": [],
                    "exception_message": msg,
                    "exception_type": "",
                    "traceback": [],
                    "current_inputs": "",
                    "current_outputs": "",
                }
                executor.add_message("execution_error", mes, broadcast=True)

            if is_master_process and ('True' == os.environ.get(DISABLE_AWS_PROXY) or not os.environ.get(DISABLE_AWS_PROXY)):
                logger.info("disabled aws proxy, use local")
                return func(*args, **kwargs)
            logger.info(f"enable aws proxy, use aws")
            executor = args[0]
            server_use = executor.server
            prompt = args[1]
            prompt_id = args[2]
            extra_data = args[3]

            client_id = extra_data['client_id'] if 'client_id' in extra_data else None
            if not client_id:
                send_error_msg(executor, prompt_id,
                               f"Something went wrong when execute,please check your client_id and try again")
                return web.Response()
            global client_release_map
            workflow_name = client_release_map.get(client_id) if client_release_map.get(client_id) else os.getenv('WORKFLOW_NAME')
            if not workflow_name or workflow_name == 'default':
                send_error_msg(executor, prompt_id, f"Please choose a cloud environment before you execute prompt")
                return web.Response()
                # if not is_master_process:
                #     send_error_msg(executor, prompt_id, f"Please choose a release env before you execute prompt")
                #     return web.Response()
                # elif not get_endpoint_name_by_workflow_name(workflow_name):
                #     send_error_msg(executor, prompt_id, f"Please check your endpoint:{get_endpoint_name_by_workflow_name(workflow_name)} before you execute prompt")
                #     return web.Response()
            # else:
                # comfy_endpoint = get_endpoint_name_by_workflow_name(workflow_name)
            logger.info(f"use endpoint:{get_endpoint_name_by_workflow_name(workflow_name)} workflow:{workflow_name} api: {api_url}to generate")

            payload = {
                "number": str(server.PromptServer.instance.number),
                "prompt": prompt,
                "prompt_id": prompt_id,
                "extra_data": extra_data,
                "endpoint_name": get_endpoint_name_by_workflow_name(workflow_name),
                "need_prepare": comfy_need_prepare,
                "need_sync": comfy_need_sync,
                "multi_async": False,
                "workflow_name": workflow_name,
            }

            def send_post_request(url, params):
                logger.debug(f"sending post request {url} , params {params}")
                get_response = requests.post(url, json=params, headers=headers)
                return get_response

            def send_get_request(url):
                get_response = requests.get(url, headers=headers)
                return get_response

            def check_if_sync_is_already(url):
                get_response = send_get_request(url)
                prepare_response = get_response.json()
                if (prepare_response['statusCode'] == 200 and 'data' in prepare_response and prepare_response['data']
                        and prepare_response['data']['prepareSuccess']):
                    logger.info(f"sync available")
                    return True
                else:
                    logger.info(f"no sync available for {url} response {prepare_response}")
                    return False

            logger.info(f"payload is: {payload}")
            # is_synced = check_if_sync_is_already(f"{api_url}/prepare/{get_endpoint_name_by_workflow_name(workflow_name)}")
            # if not is_synced:
            #     logger.debug(f"is_synced is {is_synced} stop cloud prompt")
            #     send_error_msg(executor, prompt_id,
            #                    "Your local environment has not compleated to synchronized on cloud already. Please wait for a moment or click the 'Synchronize' button .")
            #     return

            with concurrent.futures.ThreadPoolExecutor() as executorThread:
                execute_future = executorThread.submit(send_post_request, f"{api_url}/executes", payload)

                save_already = False
                if comfy_need_sync:
                    msg_future = executorThread.submit(send_get_request,
                                                       f"{api_url}/sync/{prompt_id}")
                    done, _ = concurrent.futures.wait([execute_future, msg_future],
                                                      return_when=concurrent.futures.ALL_COMPLETED)
                    already_synced = False
                    global sync_msg_list
                    sync_msg_list = []
                    for future in done:
                        if future == msg_future:
                            msg_response = future.result()
                            logger.info(f"get syc msg: {msg_response.json()}")
                            if msg_response.status_code == 200:
                                if 'data' not in msg_response.json() or not msg_response.json().get("data"):
                                    logger.error("there is no response from sync msg by thread ")
                                    time.sleep(1)
                                else:
                                    logger.debug(msg_response.json())
                                    already_synced = handle_sync_messages(server_use, msg_response.json().get("data"))
                        elif future == execute_future:
                            execute_resp = future.result()
                            logger.info(f"get execute status: {execute_resp.status_code}")
                            if execute_resp.status_code == 200 or execute_resp.status_code == 201 or execute_resp.status_code == 202:
                                i = thread_max_wait_time
                                while i > 0:
                                    images_response = send_get_request(f"{api_url}/executes/{prompt_id}")
                                    response = images_response.json()
                                    logger.info(f"get execute images: {images_response.status_code}")
                                    if images_response.status_code == 404:
                                        logger.info("no images found already ,waiting sagemaker thread result .....")
                                        time.sleep(3)
                                        i = i - 2
                                    elif response['data']['status'] == 'failed':
                                        logger.error(
                                            f"there is no response on sagemaker from execute thread result !!!!!!!! ")
                                        # send_error_msg(executor, prompt_id,
                                        #                f"There may be some errors when valid and execute the prompt on the cloud. Please check the SageMaker logs. error info: {response['data']['message']}")
                                        # no need to send msg anymore
                                        already_synced = True
                                        break
                                    elif response['data']['status'] != 'Completed' and response['data'][
                                        'status'] != 'success':
                                        logger.info(
                                            f"no images found already ,waiting sagemaker thread result, current status is {response['data']['status']}")
                                        time.sleep(2)
                                        i = i - 1
                                    elif 'data' not in response or not response['data'] or 'status' not in response[
                                        'data'] or not response['data']['status']:
                                        logger.error(
                                            f"there is no response from execute thread result !!!!!!!! {response}")
                                        # no need to send msg anymore
                                        already_synced = True
                                        # send_error_msg(executor, prompt_id,"There may be some errors when executing the prompt on cloud. No images or videos generated.")
                                        break
                                    else:
                                        if ('temp_files' in images_response.json()['data'] and len(
                                                images_response.json()['data']['temp_files']) > 0) or ((
                                                'output_files' in images_response.json()['data'] and len(
                                            images_response.json()['data']['output_files']) > 0)):
                                            logger.info(f"save images to default")
                                            save_files(prompt_id, images_response.json(), 'temp_files', './temp', False)
                                            save_files(prompt_id, images_response.json(), 'output_files', './output',
                                                       True)
                                            output_dir = folder_paths.get_output_directory()
                                            temp_dir = folder_paths.get_temp_directory()
                                            logger.info(f"save images to {output_dir} and {temp_dir}")
                                            save_files(prompt_id, images_response.json(), 'temp_files', temp_dir, False)
                                            save_files(prompt_id, images_response.json(), 'output_files', output_dir,
                                                       True)

                                        else:
                                            send_error_msg(executor, prompt_id,
                                                           "There may be some errors when executing the prompt on the cloud. Please check the SageMaker logs.")
                                            # no need to send msg anymore
                                            already_synced = True
                                        logger.debug(images_response.json())
                                        save_already = True
                                        break
                            else:
                                logger.error(f"get execute error: {execute_resp}")
                                # send_error_msg(executor, prompt_id, "Please valid your prompt and try again.")
                                # send_error_msg(executor, prompt_id,
                                #                f"There may be some errors when valid and execute the prompt on the cloud. Please check the SageMaker logs. error info: {response['data']['message']}")
                                # no need to send msg anymore
                                already_synced = True
                                break
                            logger.debug(execute_resp.json())

                    m = msg_max_wait_time
                    while not already_synced:
                        msg_response = send_get_request(f"{api_url}/sync/{prompt_id}")
                        # logger.info(msg_response.json())
                        if msg_response.status_code == 200:
                            if m <= 0:
                                logger.error("there is no response from sync msg by timeout")
                                already_synced = True
                            elif 'data' not in msg_response.json() or not msg_response.json().get("data"):
                                logger.error("there is no response from sync msg")
                                time.sleep(1)
                                m = m - 1
                            else:
                                logger.debug(msg_response.json())
                                already_synced = handle_sync_messages(server_use, msg_response.json().get("data"))
                                logger.info(f"already_synced is :{already_synced}")
                                time.sleep(1)
                                m = m - 1
                    logger.info(f"check if images are already synced {save_already}")

                if not save_already:
                    logger.info("check if images are not already synced, please wait")
                    execute_resp = execute_future.result()
                    logger.debug(f"execute result :{execute_resp.json()}")
                    if execute_resp.status_code == 200 or execute_resp.status_code == 201 or execute_resp.status_code == 202:
                        i = max_wait_time
                        while i > 0:
                            images_response = send_get_request(f"{api_url}/executes/{prompt_id}")
                            response = images_response.json()
                            logger.debug(response)
                            if images_response.status_code == 404:
                                logger.info(f"{i} no images found already ,waiting sagemaker result .....")
                                i = i - 2
                                time.sleep(3)
                            elif response['data']['status'] == 'failed':
                                logger.error(
                                    f"there is no response on sagemaker from execute result !!!!!!!! ")
                                if 'message' in response['data'] and response['data']['message']:
                                    send_error_msg(executor, prompt_id,
                                                   f"There may be some errors when valid or execute the prompt on the cloud. Please check the SageMaker logs. errors: {response['data']['message']}")
                                    break
                                else:
                                    logger.error(f"valid error on sagemaker :{response['data']}")
                                    send_error_msg(executor, prompt_id,
                                                   f"There may be some errors when valid or execute the prompt on the cloud. errors")
                                    break
                            elif response['data']['status'] != 'Completed' and response['data']['status'] != 'success':
                                logger.info(
                                    f"{i} images not already ,waiting sagemaker result .....{response['data']['status']}")
                                i = i - 1
                                time.sleep(3)
                            elif 'data' not in response or not response['data'] or 'status' not in response[
                                'data'] or not response['data']['status']:
                                logger.info(f"{i} there is no response from sync executes {response}")
                                send_error_msg(executor, prompt_id,
                                               f"There may be some errors when executing the prompt on the cloud. No images or videos generated. {response['message']}")
                                break
                            elif response['data']['status'] == 'Completed' or response['data']['status'] == 'success':
                                if ('temp_files' in images_response.json()['data'] and len(
                                        images_response.json()['data']['temp_files']) > 0) or ((
                                        'output_files' in images_response.json()['data'] and len(
                                        images_response.json()['data']['output_files']) > 0)):
                                    save_files(prompt_id, images_response.json(), 'temp_files', './temp', False)
                                    save_files(prompt_id, images_response.json(), 'output_files', './output', True)

                                    output_dir = folder_paths.get_output_directory()
                                    temp_dir = folder_paths.get_temp_directory()
                                    logger.info(f"save images to {output_dir} and {temp_dir}")
                                    save_files(prompt_id, images_response.json(), 'temp_files', temp_dir, False)
                                    save_files(prompt_id, images_response.json(), 'output_files', output_dir,
                                               True)
                                    break
                                else:
                                    send_error_msg(executor, prompt_id,
                                                   "There may be some errors when executing the prompt on the cloud. Please check the SageMaker logs.")
                                    break
                            else:
                                # logger.info(
                                #     f"{i} images not already other,waiting sagemaker result .....{response}")
                                # i = i - 1
                                # time.sleep(3)
                                send_error_msg(executor, prompt_id,
                                               "You have some errors when execute prompt on cloud . Please check your sagemaker logs.")
                                break
                    else:
                        logger.error(f"get execute error: {execute_resp.json()}")
                        send_error_msg(executor, prompt_id, "Please valid your prompt and try again." if not (execute_resp.json() and execute_resp.json().get("message")) else execute_resp.json().get("message"))
                logger.info("execute finished")
            executorThread.shutdown()

        return wrapper


    PromptExecutor.execute = execute_proxy(PromptExecutor.execute)


    def send_sync_proxy(func):
        def wrapper(*args, **kwargs):
            logger.info(f"Sending sync request----- {args}")
            return func(*args, **kwargs)

        return wrapper


    server.PromptServer.send_sync = send_sync_proxy(server.PromptServer.send_sync)


    def compress_and_upload(comfy_endpoint, folder_path, prepare_version):
        for subdir in next(os.walk(folder_path))[1]:
            subdir_path = os.path.join(folder_path, subdir)
            tar_filename = f"{subdir}.tar.gz"
            logger.info(f"Compressing the {tar_filename}")
            with tarfile.open(tar_filename, "w:gz") as tar:
                tar.add(subdir_path, arcname=os.path.basename(subdir_path))
            s5cmd_syn_node_command = f's5cmd --log=error cp {tar_filename} "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/custom_nodes/"'
            logger.info(s5cmd_syn_node_command)
            os.system(s5cmd_syn_node_command)
            logger.info(f"rm {tar_filename}")
            os.remove(tar_filename)

        # for root, dirs, files in os.walk(folder_path):
        #     for directory in dirs:
        #         dir_path = os.path.join(root, directory)
        #         logger.info(f"Compressing the {dir_path}")
        #         tar_filename = f"{directory}.tar.gz"
        #         tar_filepath = os.path.join(root, tar_filename)
        #         with tarfile.open(tar_filepath, "w:gz") as tar:
        #             tar.add(dir_path, arcname=os.path.basename(dir_path))
        #         s5cmd_syn_node_command = f's5cmd --log=error cp {tar_filepath} "s3://{bucket_name}/comfy/{comfy_endpoint}/{timestamp}/custom_nodes/"'
        #         logger.info(s5cmd_syn_node_command)
        #         os.system(s5cmd_syn_node_command)
        #         logger.info(f"rm {tar_filepath}")
        #         os.remove(tar_filepath)


    def sync_default_files(comfy_endpoint, prepare_type):
        try:
            timestamp = str(int(time.time() * 1000))
            prepare_version = PREPARE_ID if PREPARE_MODE == 'additional' else timestamp
            need_prepare = True
            need_reboot = False
            if prepare_type in ['default', 'nodes']:
                logger.info(f" sync custom nodes files")
                # s5cmd_syn_node_command = f's5cmd --log=error sync --delete=true --exclude="*comfy_local_proxy.py" {DIR2}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{timestamp}/custom_nodes/"'
                s5cmd_syn_node_command = f'aws s3 sync --delete --exclude="*comfy_local_proxy.py" {DIR2}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{timestamp}/custom_nodes/"'
                logger.info(f"sync custom_nodes files start {s5cmd_syn_node_command}")
                os.system(s5cmd_syn_node_command)
                compress_and_upload(comfy_endpoint, f"{DIR2}", prepare_version)
            if prepare_type in ['default', 'inputs']:
                logger.info(f" sync input files")
                # s5cmd_syn_input_command = f's5cmd --log=error sync --delete=true {DIR3}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/input/"'
                s5cmd_syn_input_command = f'aws s3 sync --delete {DIR3}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/input/"'
                logger.info(f"sync input files start {s5cmd_syn_input_command}")
                os.system(s5cmd_syn_input_command)
            if prepare_type in ['default', 'models']:
                logger.info(f" sync models files")
                # s5cmd_syn_model_command = f's5cmd --log=error sync --delete=true {DIR1}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/models/"'
                s5cmd_syn_model_command = f'aws s3 sync --delete {DIR1}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/models/"'
                logger.info(f"sync models files start {s5cmd_syn_model_command}")
                os.system(s5cmd_syn_model_command)
            logger.info(f"Files changed in:: {need_prepare} {prepare_type} {DIR2} {DIR1} {DIR3}")

            url = api_url + "prepare"
            logger.info(f"URL:{url}")
            data = {"endpoint_name": comfy_endpoint, "need_reboot": need_reboot, "prepare_id": prepare_version,
                    "prepare_type": prepare_type}
            logger.info(f"prepare params Data: {json.dumps(data, indent=4)}")
            result = subprocess.run(["curl", "--location", "--request", "POST", url, "--header",
                                     f"x-api-key: {api_token}", "--data-raw", json.dumps(data)],
                                    capture_output=True, text=True)
            logger.info(result.stdout)

            return result.stdout
        except Exception as e:
            logger.info(f"sync_files error {e}")
            return None


    def sync_files(filepath, is_folder, is_auto):
        comfy_endpoint = os.getenv("COMFY_ENDPOINT")
        try:
            directory = os.path.dirname(filepath)
            logger.info(f"Directory changed in: {directory} {filepath}")
            if not directory:
                logger.info("root path no need to sync files by duplicate opt")
                return None
            timestamp = str(int(time.time() * 1000))
            logger.info(f"Files changed in: {filepath} time is:{timestamp}")
            need_prepare = False
            prepare_type = 'inputs'
            need_reboot = False
            for ignore_item in no_need_sync_files:
                if filepath.endswith(ignore_item):
                    logger.info(f"no need to sync files by ignore files {filepath} ends by {ignore_item}")
                    return None
            prepare_version = PREPARE_ID if PREPARE_MODE == 'additional' else timestamp
            if (str(directory).endswith(f"{DIR2}" if DIR2.startswith("/") else f"/{DIR2}")
                    or str(filepath) == DIR2 or str(filepath) == f'./{DIR2}' or f"{DIR2}/" in filepath):
                logger.info(f" sync custom nodes files: {filepath}")
                s5cmd_syn_node_command = f's5cmd --log=error sync --delete=true --exclude="*comfy_local_proxy.py" {DIR2}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/custom_nodes/"'
                # s5cmd_syn_node_command = f'aws s3 sync {DIR2}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{timestamp}/custom_nodes/"'
                # s5cmd_syn_node_command = f's5cmd sync {DIR2}/* "s3://{bucket_name}/comfy/{comfy_endpoint}/{timestamp}/custom_nodes/"'

                # custom_node文件夹有变化 稍后再同步
                if is_auto and not is_folder_unlocked(directory):
                    logger.info("sync custom_nodes files is changing ,waiting.... ")
                    return None
                logger.info("sync custom_nodes files start")
                logger.info(s5cmd_syn_node_command)
                os.system(s5cmd_syn_node_command)
                need_prepare = True
                need_reboot = True
                prepare_type = 'nodes'
            elif (str(directory).endswith(f"{DIR3}" if DIR3.startswith("/") else f"/{DIR3}")
                  or str(filepath) == DIR3 or str(filepath) == f'./{DIR3}' or f"{DIR3}/" in filepath):
                logger.info(f" sync input files: {filepath}")
                s5cmd_syn_input_command = f'aws s3 sync --delete {DIR3}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/input/"'
                # s5cmd_syn_input_command = f'/usr/local/bin/s5cmd sync {DIR3}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/input/"'

                # 判断文件写完后再同步
                if is_auto:
                    if bool(is_folder):
                        can_sync = is_folder_unlocked(filepath)
                    else:
                        can_sync = is_file_unlocked(filepath)
                    if not can_sync:
                        logger.info("sync input files is changing ,waiting.... ")
                        return None
                logger.info("sync input files start")
                logger.info(s5cmd_syn_input_command)
                os.system(s5cmd_syn_input_command)
                # result = subprocess.run(s5cmd_syn_input_command, shell=True, check=True, stdout=subprocess.PIPE)
                # logger.info(result.stdout.decode())
                need_prepare = True
                prepare_type = 'inputs'
            elif (str(directory).endswith(f"{DIR1}" if DIR1.startswith("/") else f"/{DIR1}")
                  or str(filepath) == DIR1 or str(filepath) == f'./{DIR1}' or f"{DIR1}/" in filepath):
                logger.info(f" sync models files: {filepath}")
                s5cmd_syn_model_command = f'aws s3 sync --delete {DIR1}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/models/"'
                # s5cmd_syn_model_command = f'/usr/local/bin/s5cmd sync {DIR1}/ "s3://{bucket_name}/comfy/{comfy_endpoint}/{prepare_version}/models/"'

                # 判断文件写完后再同步
                if is_auto:
                    if bool(is_folder):
                        can_sync = is_folder_unlocked(filepath)
                    else:
                        can_sync = is_file_unlocked(filepath)
                    # logger.info(f'is folder {directory} {is_folder} can_sync {can_sync}')
                    if not can_sync:
                        logger.info("sync input models is changing ,waiting.... ")
                        return None

                logger.info("sync models files start")
                logger.info(s5cmd_syn_model_command)
                os.system(s5cmd_syn_model_command)
                # result = subprocess.run(s5cmd_syn_model_command, shell=True, check=True, stdout=subprocess.PIPE)
                # logger.info(result.stdout.decode())
                need_prepare = True
                prepare_type = 'models'
            timestamp_sync = str(int(time.time() * 1000))
            logger.info(f"Files changed in:: {need_prepare} {str(directory)} {DIR2} {DIR1} {DIR3}, time is:{timestamp_sync}")
            if need_prepare:
                url = api_url + "prepare"
                logger.info(f"URL:{url}")
                data = {"endpoint_name": comfy_endpoint, "need_reboot": need_reboot, "prepare_id": prepare_version,
                        "prepare_type": prepare_type}
                logger.info(f"prepare params Data: {json.dumps(data, indent=4)}")
                result = subprocess.run(["curl", "--location", "--request", "POST", url, "--header",
                                         f"x-api-key: {api_token}", "--data-raw", json.dumps(data)],
                                        capture_output=True, text=True)
                logger.info(result.stdout)
                timestamp_prepare = str(int(time.time() * 1000))
                logger.info(f"finish prepare in : {timestamp_prepare}")
                return result.stdout
            return None
        except Exception as e:
            logger.info(f"sync_files error {e}")
            return None


    def is_folder_unlocked(directory):
        # logger.info("check if folder ")
        event_handler = MyHandlerWithCheck()
        observer = Observer()
        observer.schedule(event_handler, directory, recursive=True)
        observer.start()
        time.sleep(1)
        result = False
        try:
            if event_handler.file_changed:
                logger.info(f"folder {directory} is still changing..")
                event_handler.file_changed = False
                time.sleep(1)
                if event_handler.file_changed:
                    logger.info(f"folder {directory} is still still changing..")
                else:
                    logger.info(f"folder {directory} changing stopped")
                    result = True
            else:
                logger.info(f"folder {directory} not stopped")
                result = True
        except (KeyboardInterrupt, Exception) as e:
            logger.info(f"folder {directory} changed exception {e}")
        observer.stop()
        return result


    def is_file_unlocked(file_path):
        # logger.info("check if file ")
        try:
            initial_size = os.path.getsize(file_path)
            initial_mtime = os.path.getmtime(file_path)
            time.sleep(1)

            current_size = os.path.getsize(file_path)
            current_mtime = os.path.getmtime(file_path)
            if current_size != initial_size or current_mtime != initial_mtime:
                logger.info(f"unlock file error {file_path} is changing")
                return False

            with open(file_path, 'r') as f:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
        except (IOError, OSError, Exception) as e:
            logger.info(f"unlock file error {file_path} is writing")
            logger.error(e)
            return False


    class MyHandlerWithCheck(FileSystemEventHandler):
        def __init__(self):
            self.file_changed = False

        def on_modified(self, event):
            logger.info(f"custom_node folder is changing {event.src_path}")
            self.file_changed = True

        def on_deleted(self, event):
            logger.info(f"custom_node folder is changing {event.src_path}")
            self.file_changed = True

        def on_created(self, event):
            logger.info(f"custom_node folder is changing {event.src_path}")
            self.file_changed = True


    class MyHandlerWithSync(FileSystemEventHandler):
        def on_modified(self, event):
            logger.info(f"{datetime.datetime.now()} files modified ，start to sync {event}")
            sync_files(event.src_path, event.is_directory, True)

        def on_created(self, event):
            logger.info(f"{datetime.datetime.now()} files added ，start to sync {event}")
            sync_files(event.src_path, event.is_directory, True)

        def on_deleted(self, event):
            logger.info(f"{datetime.datetime.now()} files deleted ，start to sync {event}")
            sync_files(event.src_path, event.is_directory, True)


    stop_event = threading.Event()

    def check_and_sync():
        logger.info("check_and_sync start")
        event_handler = MyHandlerWithSync()
        observer = Observer()
        try:
            # observer.schedule(event_handler, DIR1, recursive=True)
            # observer.schedule(event_handler, DIR2, recursive=True)
            observer.schedule(event_handler, DIR3, recursive=True)
            observer.start()
            while not stop_event.is_set():
                time.sleep(1)
        except Exception as e:
            logger.error(f"Exception in check_and_sync: {e}")
        finally:
            logger.info("Stopping observer")
            observer.stop()


    def signal_handler(sig, frame):
        logger.info("Received termination signal. Exiting...")
        stop_event.set()


    if os.environ.get('DISABLE_AUTO_SYNC') == 'false':
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        check_sync_thread = threading.Thread(target=check_and_sync)
        check_sync_thread.start()


    @server.PromptServer.instance.routes.post('/map_release')
    async def map_release(request):
        logger.info(f"start to map_release {request}")
        json_data = await request.json()
        if (not json_data or 'clientId' not in json_data or not json_data.get('clientId')
                or 'releaseVersion' not in json_data or not json_data.get('releaseVersion')):
            return web.Response(status=500, content_type='application/json', body=json.dumps({"result": False}))
        global client_release_map
        client_release_map[json_data.get('clientId')] = json_data.get('releaseVersion')
        # don‘t move used for sync automic
        os.environ['COMFY_ENDPOINT'] = get_endpoint_name_by_workflow_name(json_data.get('releaseVersion'))

        logger.info(f"client_release_map :{client_release_map}")
        return web.Response(status=200, content_type='application/json', body=json.dumps({"result": True}))


    @server.PromptServer.instance.routes.get("/reboot")
    async def restart(self):
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "action is not allowed during workflow release/restore"}))

        if not is_master_process:
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": False, "message": "only master can restart"}))
        logger.info(f"start to reboot {self}")
        try:
            from xmlrpc.client import ServerProxy
            server = ServerProxy('http://localhost:9001/RPC2')
            server.supervisor.restart()
            # server.supervisor.shutdown()
            return web.Response(status=200, content_type='application/json', body=json.dumps({"result": True}))
        except Exception as e:
            logger.info(f"error reboot  {e}")
            pass
        return os.execv(sys.executable, [sys.executable] + sys.argv)


    @server.PromptServer.instance.routes.post("/check_prepare")
    async def check_prepare(request):
        logger.info(f"start to check_prepare {request}")
        try:
            json_data = await request.json()
            comfy_endpoint = json_data['endpoint_name'] if json_data and 'endpoint_name' in json_data else None
            if not comfy_endpoint:
                workflow_name = json_data['workflow_name'] if json_data and 'workflow_name' in json_data else os.getenv(
                    'WORKFLOW_NAME')
                comfy_endpoint = get_endpoint_name_by_workflow_name(workflow_name)
            logger.info(f"check_prepare endpoint_name is:{comfy_endpoint}")
            # workflow_name = os.getenv('WORKFLOW_NAME')
            # comfy_endpoint = get_endpoint_name_by_workflow_name(workflow_name)
            get_response = requests.get(f"{api_url}/prepare/{comfy_endpoint}", headers=headers)
            response = get_response.json()
            logger.info(f"check sync response is {response}")
            if get_response.status_code == 200 and response['data']['prepareSuccess']:
                return web.Response(status=200, content_type='application/json', body=json.dumps({"result": True}))
            else:
                logger.info(f"check sync response is {response} {response['data']['prepareSuccess']}")
                return web.Response(status=500, content_type='application/json', body=json.dumps({"result": False}))
        except Exception as e:
            logger.info(f"error restart  {e}")
            pass
        return os.execv(sys.executable, [sys.executable] + sys.argv)


    @server.PromptServer.instance.routes.get("/gc")
    async def gc(self):
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "action is not allowed during workflow release/restore"}))

        logger.info(f"start to gc {self}")
        try:
            logger.info(f"gc start: {time.time()}")
            server_instance = server.PromptServer.instance
            e = execution.PromptExecutor(server_instance)
            e.reset()
            comfy.model_management.cleanup_models()
            gc.collect()
            comfy.model_management.soft_empty_cache()
            gc_triggered = True
            logger.info(f"gc end: {time.time()}")
        except Exception as e:
            logger.info(f"error restart  {e}")
            pass
        return os.execv(sys.executable, [sys.executable] + sys.argv)


    def is_action_lock():
        global lock_status
        if lock_status:
            return True
        lock_file = f'/container/sync_lock'
        if os.path.exists(lock_file):
            with open(lock_file, 'r') as f:
                content = f.read()
                if content and not check_workflow_exists(content):
                    return True
        return False


    def action_lock(name: str):
        global lock_status
        lock_status = True
        send_msg_to_all_sockets("ui_lock", {"lock": True})
        lock_file = f'/container/sync_lock'
        with open(lock_file, 'w') as f:
            f.write(name)


    def action_unlock():
        global lock_status
        lock_status = False
        send_msg_to_all_sockets("ui_lock", {"lock": False})
        lock_file = f'/container/sync_lock'
        with open(lock_file, 'w') as f:
            f.write("")


    @server.PromptServer.instance.routes.get("/restart")
    async def restart(self):
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "action is not allowed during workflow release/restore"}))
        return restart_response()

    # only for restart comfy not docker
    @server.PromptServer.instance.routes.get("/restart_comfy")
    async def restart_comfy(self):
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False,
                                     "message": "action is not allowed during workflow release/restore"}))

        thread = threading.Thread(target=restart_comfy_commands)
        thread.start()
        return web.Response(status=200, content_type='application/json',
                            body=json.dumps({"result": True, "message": "comfy will be restart in 5 seconds, "
                                                                        "it's may take a few seconds"}))


    @server.PromptServer.instance.routes.post("/sync_env")
    async def sync_env(request):
        logger.info(f"start to sync_env {request}")
        try:
            json_data = await request.json()
            prepare_type = json_data['prepare_type'] if json_data and 'prepare_type' in json_data else 'inputs'
            comfy_endpoint = json_data['endpoint_name'] if json_data and 'endpoint_name' in json_data else None
            if not comfy_endpoint:
                workflow_name = json_data['workflow_name'] if json_data and 'workflow_name' in json_data else os.getenv(
                    'WORKFLOW_NAME')
                comfy_endpoint = get_endpoint_name_by_workflow_name(workflow_name)
            logger.info(f"sync_env endpoint_name is:{comfy_endpoint}")
            global_thread_pool.submit(sync_default_files, comfy_endpoint, prepare_type)
            # thread = threading.Thread(target=sync_default_files, args=(comfy_endpoint, prepare_type))
            # thread.start()
            # result = sync_default_files()
            # logger.debug(f"sync result is :{result}")
            return web.Response(status=200, content_type='application/json', body=json.dumps({"result": True}))
        except Exception as e:
            logger.info(f"error sync_env {e}")
            pass
        return web.Response(status=500, content_type='application/json', body=json.dumps({"result": False}))


    @server.PromptServer.instance.routes.post("/change_env")
    async def change_env(request):
        logger.info(f"start to change_env {request}")
        json_data = await request.json()
        if DISABLE_AWS_PROXY in json_data and json_data[DISABLE_AWS_PROXY] is not None:
            logger.info(
                f"origin evn key DISABLE_AWS_PROXY is :{os.environ.get(DISABLE_AWS_PROXY)} {str(json_data[DISABLE_AWS_PROXY])}")
            os.environ[DISABLE_AWS_PROXY] = str(json_data[DISABLE_AWS_PROXY])
            logger.info(f"now evn key DISABLE_AWS_PROXY is :{os.environ.get(DISABLE_AWS_PROXY)}")
        return web.Response(status=200, content_type='application/json', body=json.dumps({"result": True}))


    @server.PromptServer.instance.routes.get("/get_env")
    async def get_env(request):
        env = os.environ.get(DISABLE_AWS_PROXY, 'True')
        return web.Response(status=200, content_type='application/json', body=json.dumps({"env": env}))


    @server.PromptServer.instance.routes.get("/get_env_new/{id}")
    async def get_env_new(request):
        logger.info(f"start to get_env_new {request}")
        env_key = request.match_info.get("id", None)
        logger.info("env_key is :" + str(env_key))
        env_value = os.getenv(env_key)
        return web.Response(status=200, content_type='application/json', body=json.dumps({"env": env_value}))


    @server.PromptServer.instance.routes.get("/check_is_master")
    async def check_is_master(request):
        is_master = is_master_process
        return web.Response(status=200, content_type='application/json', body=json.dumps({"master": is_master}))


    def get_cloud_workflows(workflow_name):
        response = requests.get(f"{api_url}/workflows", headers=headers, params={"limit": 1000})
        if response.status_code != 200:
            return None

        data = response.json()['data']
        workflows = data['workflows'] if (data and 'workflows' in data) else None

        if not workflows:
            return None

        for workflow in workflows:
            if workflow['name'] == workflow_name:
                return workflow['payload_json']
        return None


    @server.PromptServer.instance.routes.get("/get_env_template/{id}")
    async def get_env_template(request):
        logger.info(f"start to get_env_template {request}")
        template_id = request.match_info.get("id", None)
        logger.info("template_id is :" + str(template_id))
        workflow_name = os.getenv('WORKFLOW_NAME')
        if template_id:
            workflow_name = template_id
        if workflow_name == 'default':
            logger.info(f"workflow_name is {workflow_name}")
            return web.Response(status=500, content_type='application/json', body=None)
        prompt_json = get_cloud_workflows(workflow_name)
        logger.debug(f"workflow_name is {workflow_name} and prompt_json is: {prompt_json}")
        if not prompt_json:
            logger.info(f"get_cloud_workflows none")
            return web.Response(status=500, content_type='application/json', body=None)

        return web.Response(status=200, content_type='application/json', body=json.dumps(prompt_json))


    def get_directory_size(directory):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if not os.path.islink(filepath):
                    total_size += os.path.getsize(filepath)
        return total_size


    def dir_size(source_path: str):
        total_size_bytes = get_directory_size(source_path)
        source_size = round(total_size_bytes / (1024 ** 3), 2)
        return str(source_size)


    # def async_release_workflow(workflow_name, payload_json):
    #     start_time = time.time()
    #
    #     action_lock(workflow_name)
    #
    #     base_image = os.getenv('BASE_IMAGE')
    #     subprocess.check_output(f"echo {workflow_name} > /container/image_target_name", shell=True)
    #     subprocess.check_output(f"echo {base_image} > /container/image_base", shell=True)
    #
    #     cur_workflow_name = os.getenv('WORKFLOW_NAME')
    #     source_path = f"/container/workflows/{cur_workflow_name}"
    #     print(f"source_path is {source_path}")
    #
    #     s5cmd_sync_command = (f'aws s3 sync --quiet '
    #                           f'--delete '
    #                           f'--exclude="*comfy.tar" '
    #                           f'--exclude="*.log" '
    #                           f'--exclude="*__pycache__*" '
    #                           f'--exclude="*/ComfyUI/output/*" '
    #                           f'--exclude="*/custom_nodes/ComfyUI-Manager/*" '
    #                           f'"{source_path}/" '
    #                           f'"s3://{bucket_name}/comfy/workflows/{workflow_name}/"  --debug')
    #
    #     s5cmd_lock_command = (f'echo "lock" > lock && '
    #                           f'aws s3 cp lock s3://{bucket_name}/comfy/workflows/{workflow_name}/lock')
    #
    #     logger.info(f"sync workflows files start {s5cmd_sync_command}")
    #
    #     subprocess.check_output(s5cmd_sync_command, shell=True)
    #     subprocess.check_output(s5cmd_lock_command, shell=True)
    #
    #     end_time = time.time()
    #     cost_time = end_time - start_time
    #     image_hash = os.getenv('IMAGE_HASH')
    #     image_uri = f"{image_hash}:{workflow_name}"
    #
    #     if isinstance(payload_json, dict):
    #         payload_json = json.dumps(payload_json)
    #
    #     data = {
    #         "payload_json": payload_json,
    #         "image_uri": image_uri,
    #         "name": workflow_name,
    #         "size": dir_size(source_path),
    #     }
    #     get_response = requests.post(f"{api_url}/workflows", headers=headers, data=json.dumps(data))
    #     response = get_response.json()
    #     logger.info(f"release workflow response is {response}")
    #     action_unlock()
    #     print(f"release workflow cost time is {cost_time}")


    @server.PromptServer.instance.routes.get("/lock")
    async def get_lock_status(request):
        return web.Response(status=200, content_type='application/json',
                            body=json.dumps({"result": True, "lock": is_action_lock()}))


    def async_release_env(workflow_name, payload_json, init_count: int, instance_type, auto_scale: bool, min_count: int, max_count: int):
        try:
            start_time = time.time()
            action_lock(workflow_name)
            base_image = os.getenv('BASE_IMAGE')
            subprocess.check_output(f"echo {workflow_name} > /container/image_target_name", shell=True)
            subprocess.check_output(f"echo {base_image} > /container/image_base", shell=True)

            cur_workflow_name = os.getenv('WORKFLOW_NAME')
            source_path = f"/container/workflows/{cur_workflow_name}"
            print(f"source_path is {source_path}")

            s5cmd_sync_command = (f'aws s3 sync --quiet '
                                  f'--delete '
                                  f'--exclude="*comfy.tar" '
                                  f'--exclude="*.log" '
                                  f'--exclude="*__pycache__*" '
                                  f'--exclude="*/ComfyUI/output/*" '
                                  f'--exclude="*/custom_nodes/ComfyUI-Manager/*" '
                                  f'"{source_path}/" '
                                  f'"s3://{bucket_name}/comfy/workflows/{workflow_name}/"')

            s5cmd_lock_command = (f'echo "lock" > lock && '
                                  f'aws s3 cp lock s3://{bucket_name}/comfy/workflows/{workflow_name}/lock')

            logger.info(f"sync workflows files start {s5cmd_sync_command}")

            subprocess.check_output(s5cmd_sync_command, shell=True)
            subprocess.check_output(s5cmd_lock_command, shell=True)

            end_time = time.time()
            cost_time = end_time - start_time
            image_hash = os.getenv('IMAGE_HASH')
            image_uri = f"{image_hash}:{workflow_name}"

            if isinstance(payload_json, dict):
                payload_json = json.dumps(payload_json)

            data = {
                "payload_json": payload_json,
                "image_uri": image_uri,
                "name": workflow_name,
                "size": dir_size(source_path),
            }
            get_response = requests.post(f"{api_url}/workflows", headers=headers, data=json.dumps(data))
            response = get_response.json()
            logger.info(f"release workflow response is {response}")
            # TODO check response
            endpoint_data = {
                'workflow_name': workflow_name,
                'endpoint_name': '',
                'service_type': 'comfy',
                'endpoint_type': 'Async',
                'instance_type': instance_type,
                'initial_instance_count': init_count,
                'min_instance_number': min_count,
                'max_instance_number': max_count,
                'autoscaling_enabled': auto_scale,
                'assign_to_roles': [f'ec2{workflow_name}'],
            }
            endpoint_response = requests.post(f"{api_url}/endpoints", headers=headers, data=json.dumps(endpoint_data))
            logger.info(f"release env endpoint response is {endpoint_response}")
            action_unlock()
            logger.info(f"release workflow cost time is {cost_time}")
        except Exception as e:
            action_unlock()
            logger.info(f"release workflow error start to rm lock:{e} ")


    @server.PromptServer.instance.routes.post("/release")
    async def release_env(request):
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False,
                                     "message": "action is not allowed during workflow release/restore"}))

        if not is_master_process:
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": False, "message": "only master can release workflow"}))

        logger.info(f"start to release workflow {request}")
        try:
            json_data = await request.json()
            if 'name' not in json_data or not json_data['name']:
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"name is required"}))

            workflow_name = json_data['name']
            if workflow_name == 'default' or workflow_name == 'local':
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"{workflow_name} is not allowed"}))

            if not re.match(r'^[A-Za-z][A-Za-z0-9]*$', workflow_name):
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"{workflow_name} is invalid name"}))

            payload_json = ''

            if 'payload_json' in json_data:
                payload_json = json_data['payload_json']

            if check_workflow_exists(workflow_name):
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"{workflow_name} already exists"}))

            if ('initCount' not in json_data or not json_data['initCount']
                    or not json_data['initCount'].isdigit() or int(json_data['initCount']) <= 0):
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"initCount is required"}))
            if ('autoScale' not in json_data or not json_data['autoScale']):
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"autoScale is required"}))
            if 'autoScale' in json_data and json_data['autoScale']:
                if ('minCount' not in json_data or not json_data['minCount']
                        or not json_data['minCount'].isdigit() or int(json_data['minCount']) < 0):
                    return web.Response(status=200, content_type='application/json',
                                        body=json.dumps({"result": False, "message": f"minCount is required"}))
                if ('maxCount' not in json_data or not json_data['maxCount']
                        or not json_data['maxCount'].isdigit() or int(json_data['maxCount']) < 0):
                    return web.Response(status=200, content_type='application/json',
                                        body=json.dumps({"result": False, "message": f"maxCount is required"}))

            thread = threading.Thread(target=async_release_env, args=(workflow_name, payload_json, int(json_data['initCount']), json_data['instanceType'], bool(json_data['autoScale']), int(json_data['minCount']), int(json_data['maxCount'])))
            thread.start()

            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": True, "message": "Pending to release, it may take a few minutes"}))
        except Exception as e:
            logger.info(e)
            action_unlock()
            return web.Response(status=500, content_type='application/json',
                                body=json.dumps({"result": False, "message": 'Release workflow failed.'}))

    @server.PromptServer.instance.routes.delete("/workflows")
    async def delete_workflow(request):
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "Action is not allowed during workflow release/restore"}))

        if not is_master_process:
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": False, "message": "Only master can delete workflows"}))

        logger.info(f"start to delete workflows {request}")
        try:
            json_data = await request.json()
            if 'name' not in json_data or not json_data['name']:
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"name is required"}))
            name = json_data['name']

            if name == 'default' or name == 'local':
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"{name} is not allowed"}))

            if os.getenv('WORKFLOW_NAME') == name:
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": "Cannot delete current workflow."}))

            i = 0
            start_n = 10000
            while i < 30:
                port = start_n + i
                file_path = f"/container/comfy_{port}"
                i = i + 1
                if os.path.exists(file_path):
                    with open(file_path, 'r') as f:
                        content = f.read()
                        content = content.strip()
                        if content == name:
                            return web.Response(status=200, content_type='application/json',
                                                body=json.dumps({"result": False,
                                                                 "message": f"Cannot delete workflow, it is in use by port {port}."}))

            data = {
                "workflow_name_list": [name],
            }
            response = requests.delete(f"{api_url}/workflows", headers=headers, data=json.dumps(data))
            resp = response.json()
            if response.status_code != 202:
                return web.Response(status=200,
                                    content_type='application/json',
                                    body=json.dumps({"result": False, "message": resp['message']}))

            os.system(f"rm -rf /container/workflows/{name}")

            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": True, "message": "Workflow will be deleted soon"}))
        except Exception as e:
            logger.info(e)
            return web.Response(status=500, content_type='application/json',
                                body=json.dumps({"result": False, "message": 'Delete workflow failed'}))

    @server.PromptServer.instance.routes.put("/workflows")
    async def switch_workflow(request):
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "Switch is not allowed during workflow release/restore"}))

        if os.path.exists("/container/s5cmd_lock"):
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "Switch is not allowed during other's switch, "
                                                                 "please try again later"}))

        try:
            json_data = await request.json()
            if 'name' not in json_data or not json_data['name']:
                raise ValueError("name is required")

            workflow_name = json_data['name']

            if workflow_name == os.getenv('WORKFLOW_NAME'):
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": "Workflow is already in use"}))

            if workflow_name == 'default' and not is_master_process:
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": "Slave cannot use default after initial"}))

            if workflow_name != 'default':
                if not check_workflow_exists(workflow_name):
                    return web.Response(status=200, content_type='application/json',
                                        body=json.dumps({"result": False, "message": f"{workflow_name} not exists"}))

            name_file = os.getenv('WORKFLOW_NAME_FILE')

            # don‘t move used for sync automic
            os.environ['COMFY_ENDPOINT'] = get_endpoint_name_by_workflow_name(workflow_name)

            subprocess.check_output(f"echo {workflow_name} > {name_file}", shell=True)

            thread = threading.Thread(target=kill_after_seconds)
            thread.start()
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": True, "message": "Comfy workflow will switch in 2 seconds, it may take a few minutes."}))
        except Exception as e:
            logger.info(e)
            return web.Response(status=500, content_type='application/json',
                                body=json.dumps({"result": False, "message": 'Switch workflow failed'}))

    @server.PromptServer.instance.routes.get("/workflows")
    async def get_workflows(request):
        try:
            workflow_name = os.getenv('WORKFLOW_NAME')

            response = requests.get(f"{api_url}/workflows", headers=headers, params={"limit": 1000})
            if response.status_code != 200:
                return web.Response(status=500, content_type='application/json',
                                    body=json.dumps({"result": False, "message": 'Get workflows failed'}))

            data = response.json()['data']
            workflows = data['workflows']

            list = []

            if is_master_process:
                list.append({
                    "name": 'default',
                    "in_use": 'default' == workflow_name,
                    "size": dir_size(f"/container/workflows/default"),
                    "status": 'Enabled',
                    "payload_json": '',
                })

            for workflow in workflows:
                list.append({
                    "name": workflow['name'],
                    "in_use": workflow['name'] == workflow_name,
                    "size": workflow['size'],
                    "status": workflow['status'],
                    "payload_json": workflow['payload_json'],
                })

            data['workflows'] = list

            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": True, "data": data}))
        except Exception as e:
            logger.info(e)
            return web.Response(status=500, content_type='application/json',
                                body=json.dumps({"result": False, "message": 'list workflows failed'}))

    @server.PromptServer.instance.routes.get("/schemas")
    async def get_schemas(request):
        try:

            limit = 10
            if 'limit' in request.query and request.query['limit']:
                limit = int(request.query['limit'])

            exclusive_start_key = None
            if 'exclusive_start_key' in request.query and request.query['exclusive_start_key']:
                exclusive_start_key = request.query['exclusive_start_key']

            params={
                "limit": limit,
                "exclusive_start_key": exclusive_start_key,
            }

            response = requests.get(f"{api_url}/schemas", headers=headers, params=params)

            if response.status_code != 200:
                resp = response.json()
                return web.Response(status=500, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f'List schemas failed: {resp["message"]}'}))

            data = response.json()['data']
            schemas = data['schemas']

            list = [{"name": "default(🔒)", "workflow": "default", "payload": "", "create_time": time.time()}]

            for schema in schemas:
                if not is_master_process and not schema['workflow']:
                    continue

                list.append({
                    "name": schema['name'],
                    "workflow": schema['workflow'],
                    "payload": schema['payload'],
                    "create_time": schema['create_time'],
                })

            data['schemas'] = list

            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": True, "data": data}))
        except Exception as e:
            return web.Response(status=500, content_type='application/json',
                                body=json.dumps({"result": False, "message": f'List schemas failed: {e}'}))

    @server.PromptServer.instance.routes.post("/schemas")
    async def create_schema(request):
        if not is_master_process:
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": False, "message": "only master can create schema"}))

        try:
            json_data = await request.json()
            if 'name' not in json_data or not json_data['name']:
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"name is required"}))

            if json_data['name'] == "default(🔒)":
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"named 'default' cannot be created"}))

            if 'payload' not in json_data or not json_data['payload']:
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"payload is required"}))

            name = json_data['name']
            payload = json_data['payload']
            workflow_name = ''

            if 'workflow' in json_data and json_data['workflow']:
                workflow_name = json_data['workflow']

            data = {
                "payload": payload,
                "name": name,
                "workflow": workflow_name,
            }
            get_response = requests.post(f"{api_url}/schemas", headers=headers, data=json.dumps(data))
            response = get_response.json()
            if get_response.status_code != 200:
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": response['message']}))

            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": True, "message": "Created schema"}))
        except Exception as e:
            logger.info(e)
            return web.Response(status=500, content_type='application/json',
                                body=json.dumps({"result": False, "message": 'Create schema failed'}))

    @server.PromptServer.instance.routes.delete("/schemas")
    async def delete_schemas(request):
        if not is_master_process:
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": False, "message": "only master can delete schemas"}))

        try:
            json_data = await request.json()
            if 'schema_name_list' not in json_data or not json_data['schema_name_list']:
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"schema_name_list is required"}))
            schema_name_list = json_data['schema_name_list']

            data = {
                "schema_name_list": schema_name_list,
            }
            response = requests.delete(f"{api_url}/schemas", headers=headers, data=json.dumps(data))

            if response.status_code != 204:
                resp = response.json()
                return web.Response(status=200,
                                    content_type='application/json',
                                    body=json.dumps({"result": False, "message": resp['message']}))

            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": True, "message": "Schema deleted"}))
        except Exception as e:
            logger.info(e)
            return web.Response(status=500, content_type='application/json',
                                body=json.dumps({"result": False, "message": 'Delete schemas failed'}))

    @server.PromptServer.instance.routes.put("/schemas")
    async def update_schema(request):
        if not is_master_process:
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": False, "message": "only master can update schema"}))

        try:
            json_data = await request.json()
            if 'name' not in json_data or not json_data['name']:
                raise ValueError("name is required")
            if json_data['name'] == "default(🔒)":
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": f"named 'default' cannot be edited"}))
            if 'payload' not in json_data or not json_data['payload']:
                raise ValueError("payload is required")

            workflow_name = ''

            if 'workflow' in json_data and json_data['workflow']:
                workflow_name = json_data['workflow']

            name = json_data['name']
            payload = json_data['payload']

            data = {
                "workflow": workflow_name,
                "payload": payload,
            }
            get_response = requests.put(f"{api_url}/schemas/{name}", headers=headers, data=json.dumps(data))
            if get_response.status_code != 200:
                response = get_response.json()
                return web.Response(status=200, content_type='application/json',
                                    body=json.dumps({"result": False, "message": response['message']}))

            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": True, "message": "Updated schema"}))
        except Exception as e:
            logger.info(e)
            return web.Response(status=500, content_type='application/json',
                                body=json.dumps({"result": False, "message": f'Update schema failed: {e}'}))

    def check_workflow_exists(name: str):
        get_response = requests.get(f"{api_url}/workflows/{name}", headers=headers)
        return get_response.status_code == 200


    def restart_docker_commands():
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "action is not allowed during release workflow"}))

        subprocess.run(["sleep", "5"])
        kill_python_process()


    def restart_comfy_commands():
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "action is not allowed during release workflow"}))
        try:
            sys.stdout.close_log()
        except Exception as e:
            logger.info(f"error restart  {e}")
            pass
        return os.execv(sys.executable, [sys.executable] + sys.argv)


    def restart_response():
        thread = threading.Thread(target=restart_docker_commands)
        thread.start()
        return web.Response(status=200, content_type='application/json',
                            body=json.dumps({"result": True, "message": "comfy will be restart in 5 seconds"}))

    def kill_after_seconds():
        subprocess.run(["sleep", "2"])
        kill_python_process()


    def kill_python_process():
        subprocess.run(["pkill", "-f", "python3"])

        result = subprocess.run(["ps", "-ef"], stdout=subprocess.PIPE)
        lines = result.stdout.decode().splitlines()

        for line in lines:
            if "venv/bin/python3 main.py" in line and "--listen" in line:
                parts = line.split()
                pid = int(parts[1])
                print(f"Killing process with PID: {pid}")
                os.kill(pid, 9)

    def restore_workflow():
        action_lock("restore")
        subprocess.run(["sleep", "2"])
        os.system("rm -rf /container/workflows/default")
        action_unlock()
        kill_python_process()

    @server.PromptServer.instance.routes.post("/restore")
    async def release_rebuild_workflow(request):
        if is_action_lock():
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps(
                                    {"result": False, "message": "action is not allowed during workflow release/restore"}))

        if os.getenv('WORKFLOW_NAME') != 'default':
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": False, "message": "only default workflow can be restored"}))

        if not is_master_process:
            return web.Response(status=200, content_type='application/json',
                                body=json.dumps({"result": False, "message": "only master can restore comfy"}))

        thread = threading.Thread(target=restore_workflow)
        thread.start()
        return web.Response(status=200, content_type='application/json',
                            body=json.dumps({"result": True, "message": "Comfy will be restored in 2 seconds,it may take a few minutes"}))

if is_on_sagemaker:

    global need_sync
    global prompt_id
    global executing
    executing = False

    global reboot
    reboot = False

    global last_call_time
    last_call_time = None
    global gc_triggered
    gc_triggered = False

    REGION = os.environ.get('AWS_REGION')
    BUCKET = os.environ.get('S3_BUCKET_NAME')
    QUEUE_URL = os.environ.get('COMFY_QUEUE_URL')

    GEN_INSTANCE_ID = os.environ.get('ENDPOINT_INSTANCE_ID') if 'ENDPOINT_INSTANCE_ID' in os.environ and os.environ.get(
        'ENDPOINT_INSTANCE_ID') else str(uuid.uuid4())
    ENDPOINT_NAME = os.environ.get('ENDPOINT_NAME')
    ENDPOINT_ID = os.environ.get('ENDPOINT_ID')

    INSTANCE_MONITOR_TABLE_NAME = os.environ.get('COMFY_INSTANCE_MONITOR_TABLE')
    SYNC_TABLE_NAME = os.environ.get('COMFY_SYNC_TABLE')

    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    sync_table = dynamodb.Table(SYNC_TABLE_NAME)
    instance_monitor_table = dynamodb.Table(INSTANCE_MONITOR_TABLE_NAME)

    logger = logging.getLogger(__name__)
    logger.setLevel(os.environ.get('LOG_LEVEL') or logging.INFO)

    ROOT_PATH = '/home/ubuntu/ComfyUI'
    sqs_client = boto3.client('sqs', region_name=REGION)

    GC_WAIT_TIME = 1800


    def print_env():
        for key, value in os.environ.items():
            logger.info(f"{key}: {value}")


    @dataclass
    class ComfyResponse:
        statusCode: int
        message: str
        body: Optional[dict]


    def ok(body: dict):
        return web.Response(status=200, content_type='application/json', body=json.dumps(body))


    def error(body: dict):
        # TODO 500 -》200 because of need resp anyway not exception
        return web.Response(status=200, content_type='application/json', body=json.dumps(body))


    def sen_sqs_msg(message_body, prompt_id_key):
        response = sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(message_body),
            MessageGroupId=prompt_id_key
        )
        message_id = response['MessageId']
        return message_id


    def sen_finish_sqs_msg(prompt_id_key):
        global need_sync
        # logger.info(f"sen_finish_sqs_msg start... {need_sync},{prompt_id_key}")
        if need_sync and QUEUE_URL and REGION:
            message_body = {'prompt_id': prompt_id_key, 'event': 'finish',
                            'data': {"node": None, "prompt_id": prompt_id_key},
                            'sid': None}
            message_id = sen_sqs_msg(message_body, prompt_id_key)
            logger.info(f"finish message sent {message_id}")


    async def prepare_comfy_env(sync_item: dict):
        try:
            request_id = sync_item['request_id']
            logger.info(f"prepare_environment start sync_item:{sync_item}")
            prepare_type = sync_item['prepare_type']
            rlt = True
            if prepare_type in ['default', 'models']:
                sync_models_rlt = sync_s3_files_or_folders_to_local(f'{request_id}/models/*', f'{ROOT_PATH}/models',
                                                                    False)
                if not sync_models_rlt:
                    rlt = False
            if prepare_type in ['default', 'inputs']:
                sync_inputs_rlt = sync_s3_files_or_folders_to_local(f'{request_id}/input/*', f'{ROOT_PATH}/input',
                                                                    False)
                if not sync_inputs_rlt:
                    rlt = False
            if prepare_type in ['nodes']:
                sync_nodes_rlt = sync_s3_files_or_folders_to_local(f'{request_id}/custom_nodes/*',
                                                                   f'{ROOT_PATH}/custom_nodes', True)
                if not sync_nodes_rlt:
                    rlt = False
            if prepare_type == 'custom':
                sync_source_path = sync_item['s3_source_path']
                local_target_path = sync_item['local_target_path']
                if not sync_source_path or not local_target_path:
                    logger.info("s3_source_path and local_target_path should not be empty")
                else:
                    sync_rlt = sync_s3_files_or_folders_to_local(sync_source_path,
                                                                 f'{ROOT_PATH}/{local_target_path}', False)
                    if not sync_rlt:
                        rlt = False
            elif prepare_type == 'other':
                sync_script = sync_item['sync_script']
                logger.info(f"sync_script {sync_script} sync_item: {sync_item}")
                # sync_script.startswith('s5cmd') 不允许
                try:
                    if sync_script and (
                            sync_script.startswith("cat") or sync_script.startswith("os.environ")
                            or sync_script.startswith("print") or sync_script.startswith("ls ")
                            or sync_script.startswith("du ")
                            or sync_script.startswith("python3 -m pip") or sync_script.startswith("python -m pip")
                            or sync_script.startswith("pip install") or sync_script.startswith("apt")
                            or sync_script.startswith("curl") or sync_script.startswith("wget")
                            or sync_script.startswith("env") or sync_script.startswith("source")
                            or sync_script.startswith("sudo chmod") or sync_script.startswith("chmod")
                            or sync_script.startswith("/home/ubuntu/ComfyUI/venv/bin/python")
                    ):
                        os.system(sync_script)
                    elif sync_script and (sync_script.startswith("export ") and len(sync_script.split(" ")) > 2):
                        sync_script_key = sync_script.split(" ")[1]
                        sync_script_value = sync_script.split(" ")[2]
                        os.environ[sync_script_key] = sync_script_value
                        logger.info(os.environ.get(sync_script_key))
                except Exception as e:
                    logger.error(f"Exception while execute sync_scripts : {sync_script}")
                    rlt = False
            need_reboot = True if ('need_reboot' in sync_item and sync_item['need_reboot']
                                   and str(sync_item['need_reboot']).lower() == 'true') else False
            global reboot
            reboot = need_reboot
            if need_reboot:
                os.environ['NEED_REBOOT'] = 'true'
            else:
                os.environ['NEED_REBOOT'] = 'false'
            logger.info("prepare_environment end")
            os.environ['LAST_SYNC_REQUEST_ID'] = sync_item['request_id']
            os.environ['LAST_SYNC_REQUEST_TIME'] = str(sync_item['request_time'])
            return rlt
        except Exception as e:
            return False


    def sync_s3_files_or_folders_to_local(s3_path, local_path, need_un_tar):
        logger.info("sync_s3_models_or_inputs_to_local start")
        # s5cmd_command = f'{ROOT_PATH}/tools/s5cmd sync "s3://{bucket_name}/{s3_path}/*" "{local_path}/"'
        if need_un_tar:
            s5cmd_command = f's5cmd sync "s3://{BUCKET}/comfy/{ENDPOINT_NAME}/{s3_path}" "{local_path}/"'
        else:
            s5cmd_command = f's5cmd sync --delete=true "s3://{BUCKET}/comfy/{ENDPOINT_NAME}/{s3_path}" "{local_path}/"'
        # s5cmd_command = f's5cmd sync --delete=true "s3://{BUCKET}/comfy/{ENDPOINT_NAME}/{s3_path}" "{local_path}/"'
        # s5cmd_command = f's5cmd sync "s3://{BUCKET}/comfy/{ENDPOINT_NAME}/{s3_path}" "{local_path}/"'
        try:
            logger.info(s5cmd_command)
            os.system(s5cmd_command)
            logger.info(f'Files copied from "s3://{BUCKET}/comfy/{ENDPOINT_NAME}/{s3_path}" to "{local_path}/"')
            if need_un_tar:
                for filename in os.listdir(local_path):
                    if filename.endswith(".tar.gz"):
                        tar_filepath = os.path.join(local_path, filename)
                        # extract_path = os.path.splitext(os.path.splitext(tar_filepath)[0])[0]
                        # os.makedirs(extract_path, exist_ok=True)
                        # logger.info(f'Extracting extract_path is {extract_path}')

                        with tarfile.open(tar_filepath, "r:gz") as tar:
                            for member in tar.getmembers():
                                tar.extract(member, path=local_path)
                        os.remove(tar_filepath)
                        logger.info(f'File {tar_filepath} extracted and removed')
            return True
        except Exception as e:
            logger.info(f"Error executing s5cmd command: {e}")
            return False


    def sync_local_outputs_to_s3(s3_path, local_path):
        logger.info("sync_local_outputs_to_s3 start")
        s5cmd_command = f's5cmd sync "{local_path}/*" "s3://{BUCKET}/comfy/{s3_path}/" '
        try:
            logger.info(s5cmd_command)
            os.system(s5cmd_command)
            logger.info(f'Files copied local to "s3://{BUCKET}/comfy/{s3_path}/" to "{local_path}/"')
            clean_cmd = f'rm -rf {local_path}'
            os.system(clean_cmd)
            logger.info(f'Files removed from local {local_path}')
        except Exception as e:
            logger.info(f"Error executing s5cmd command: {e}")


    def sync_local_outputs_to_base64(local_path):
        logger.info("sync_local_outputs_to_base64 start")
        try:
            result = {}
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    with open(file_path, "rb") as f:
                        file_content = f.read()
                        base64_content = base64.b64encode(file_content).decode('utf-8')
                        result[file] = base64_content
            clean_cmd = f'rm -rf {local_path}'
            os.system(clean_cmd)
            logger.info(f'Files removed from local {local_path}')
            return result
        except Exception as e:
            logger.info(f"Error executing s5cmd command: {e}")
            return {}


    @server.PromptServer.instance.routes.post("/execute_proxy")
    async def execute_proxy(request):
        logger.info("start to execute_proxy inside")
        json_data = await request.json()
        if 'out_path' in json_data and json_data['out_path'] is not None:
            out_path = json_data['out_path']
        else:
            out_path = None
        logger.info(f"invocations start json_data:{json_data}")
        global need_sync
        need_sync = json_data["need_sync"]
        global prompt_id
        prompt_id = json_data["prompt_id"]
        try:
            global executing
            if executing is True:
                resp = {"prompt_id": prompt_id, "instance_id": GEN_INSTANCE_ID, "status": "fail",
                        "message": "the environment is not ready valid[0] is false, need to resync"}
                sen_finish_sqs_msg(prompt_id)
                return error(resp)
            executing = True
            logger.info(
                f'bucket_name: {BUCKET}, region: {REGION}')
            if ('need_prepare' in json_data and json_data['need_prepare']
                    and 'prepare_props' in json_data and json_data['prepare_props']):
                sync_already = await prepare_comfy_env(json_data['prepare_props'])
                if not sync_already:
                    resp = {"prompt_id": prompt_id, "instance_id": GEN_INSTANCE_ID, "status": "fail",
                            "message": "the environment is not ready with sync"}
                    executing = False
                    sen_finish_sqs_msg(prompt_id)
                    return error(resp)
            server_instance = server.PromptServer.instance
            if "number" in json_data:
                number = float(json_data['number'])
                server_instance.number = number
            else:
                number = server_instance.number
                if "front" in json_data:
                    if json_data['front']:
                        number = -number
                server_instance.number += 1
            valid = execution.validate_prompt(json_data['prompt'])
            logger.info(f"Validating prompt result is {valid}")
            if not valid[0]:
                resp = {"prompt_id": prompt_id, "instance_id": GEN_INSTANCE_ID, "status": "fail",
                        "message": "the environment is not ready valid[0] is false, need to resync"}
                executing = False
                response = {"prompt_id": prompt_id, "number": number, "node_errors": valid[3]}
                sen_finish_sqs_msg(prompt_id)
                return error(resp)
            # if len(valid) == 4 and len(valid[3]) > 0:
            #     logger.info(f"Validating prompt error there is something error because of :valid: {valid}")
            #     resp = {"prompt_id": prompt_id, "instance_id": GEN_INSTANCE_ID, "status": "fail",
            #             "message": f"the valid is error, need to resync or check the workflow :{valid}"}
            #     executing = False
            #     return error(resp)
            extra_data = {}
            client_id = ''
            if "extra_data" in json_data:
                extra_data = json_data["extra_data"]
                if 'client_id' in extra_data and extra_data['client_id']:
                    client_id = extra_data['client_id']
            if "client_id" in json_data and json_data["client_id"]:
                extra_data["client_id"] = json_data["client_id"]
                client_id = json_data["client_id"]

            server_instance.client_id = client_id

            prompt_id = json_data['prompt_id']
            server_instance.last_prompt_id = prompt_id
            e = execution.PromptExecutor(server_instance)
            outputs_to_execute = valid[2]
            e.execute(json_data['prompt'], prompt_id, extra_data, outputs_to_execute)

            s3_out_path = f'output/{prompt_id}/{out_path}' if out_path is not None else f'output/{prompt_id}'
            s3_temp_path = f'temp/{prompt_id}/{out_path}' if out_path is not None else f'temp/{prompt_id}'
            local_out_path = f'{ROOT_PATH}/output/{out_path}' if out_path is not None else f'{ROOT_PATH}/output'
            local_temp_path = f'{ROOT_PATH}/temp/{out_path}' if out_path is not None else f'{ROOT_PATH}/temp'

            logger.info(
                f"s3_out_path is {s3_out_path} and s3_temp_path is {s3_temp_path} and local_out_path is {local_out_path} and local_temp_path is {local_temp_path}")

            sync_local_outputs_to_s3(s3_out_path, local_out_path)
            sync_local_outputs_to_s3(s3_temp_path, local_temp_path)

            response_body = {
                "prompt_id": prompt_id,
                "instance_id": GEN_INSTANCE_ID,
                "status": "success",
                "output_path": f's3://{BUCKET}/comfy/{s3_out_path}',
                "temp_path": f's3://{BUCKET}/comfy/{s3_temp_path}',
            }
            sen_finish_sqs_msg(prompt_id)
            logger.info(f"execute inference response is {response_body}")
            executing = False
            return ok(response_body)
        except Exception as ecp:
            logger.info(f"exception occurred {ecp}")
            resp = {"prompt_id": prompt_id, "instance_id": GEN_INSTANCE_ID, "status": "fail",
                    "message": f"exception occurred {ecp}"}
            executing = False
            return error(resp)
        finally:
            logger.info(f"gc check: {time.time()}")
            try:
                global last_call_time, gc_triggered
                gc_triggered = False
                if last_call_time is None:
                    logger.info(f"gc check last time is NONE")
                    last_call_time = time.time()
                else:
                    if time.time() - last_call_time > GC_WAIT_TIME:
                        if not gc_triggered:
                            logger.info(f"gc start: {time.time()} - {last_call_time}")
                            e.reset()
                            comfy.model_management.cleanup_models()
                            gc.collect()
                            comfy.model_management.soft_empty_cache()
                            gc_triggered = True
                            logger.info(f"gc end: {time.time()} - {last_call_time}")
                        last_call_time = time.time()
                    else:
                        last_call_time = time.time()
                logger.info(f"gc check end: {time.time()}")
            except Exception as e:
                logger.info(f"gc error: {e}")


    def get_last_ddb_sync_record():
        sync_response = sync_table.query(
            KeyConditionExpression=Key('endpoint_name').eq(ENDPOINT_NAME),
            Limit=1,
            ScanIndexForward=False
        )
        latest_sync_record = sync_response['Items'][0] if ('Items' in sync_response
                                                           and len(sync_response['Items']) > 0) else None
        if latest_sync_record:
            logger.info(f"latest_sync_record is：{latest_sync_record}")
            return latest_sync_record

        logger.info("no latest_sync_record found")
        return None


    def get_latest_ddb_instance_monitor_record():
        key_condition_expression = ('endpoint_name = :endpoint_name_val '
                                    'AND gen_instance_id = :gen_instance_id_val')
        expression_attribute_values = {
            ':endpoint_name_val': ENDPOINT_NAME,
            ':gen_instance_id_val': GEN_INSTANCE_ID
        }
        instance_monitor_response = instance_monitor_table.query(
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
        instance_monitor_record = instance_monitor_response['Items'][0] \
            if ('Items' in instance_monitor_response and len(instance_monitor_response['Items']) > 0) else None

        if instance_monitor_record:
            logger.info(f"instance_monitor_record is {instance_monitor_record}")
            return instance_monitor_record

        logger.info("no instance_monitor_record found")
        return None


    def save_sync_instance_monitor(last_sync_request_id: str, sync_status: str):
        item = {
            'endpoint_id': ENDPOINT_ID,
            'endpoint_name': ENDPOINT_NAME,
            'gen_instance_id': GEN_INSTANCE_ID,
            'sync_status': sync_status,
            'last_sync_request_id': last_sync_request_id,
            'last_sync_time': datetime.datetime.now().isoformat(),
            'sync_list': [],
            'create_time': datetime.datetime.now().isoformat(),
            'last_heartbeat_time': datetime.datetime.now().isoformat()
        }
        save_resp = instance_monitor_table.put_item(Item=item)
        logger.info(f"save instance item {save_resp}")
        return save_resp


    def update_sync_instance_monitor(instance_monitor_record):
        update_expression = ("SET sync_status = :new_sync_status, last_sync_request_id = :sync_request_id, "
                             "sync_list = :sync_list, last_sync_time = :sync_time, last_heartbeat_time = :heartbeat_time")
        expression_attribute_values = {
            ":new_sync_status": instance_monitor_record['sync_status'],
            ":sync_request_id": instance_monitor_record['last_sync_request_id'],
            ":sync_list": instance_monitor_record['sync_list'],
            ":sync_time": datetime.datetime.now().isoformat(),
            ":heartbeat_time": datetime.datetime.now().isoformat(),
        }

        response = instance_monitor_table.update_item(
            Key={'endpoint_name': ENDPOINT_NAME,
                 'gen_instance_id': GEN_INSTANCE_ID},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
        logger.info(f"update_sync_instance_monitor :{response}")
        return response


    def sync_instance_monitor_status(need_save: bool):
        try:
            logger.info(f"sync_instance_monitor_status {datetime.datetime.now()}")
            if need_save:
                save_sync_instance_monitor('', 'init')
            else:
                update_expression = ("SET last_heartbeat_time = :heartbeat_time")
                expression_attribute_values = {
                    ":heartbeat_time": datetime.datetime.now().isoformat(),
                }
                instance_monitor_table.update_item(
                    Key={'endpoint_name': ENDPOINT_NAME,
                         'gen_instance_id': GEN_INSTANCE_ID},
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_attribute_values
                )
        except Exception as e:
            logger.info(f"sync_instance_monitor_status error :{e}")


    @server.PromptServer.instance.routes.post("/reboot")
    async def restart(self):
        logger.debug(f"start to reboot!!!!!!!! {self}")
        global executing
        if executing is True:
            logger.info(f"other inference doing cannot reboot!!!!!!!!")
            return ok({"message": "other inference doing cannot reboot"})
        need_reboot = os.environ.get('NEED_REBOOT')
        if need_reboot and need_reboot.lower() != 'true':
            logger.info("no need to reboot by os")
            return ok({"message": "no need to reboot by os"})
        global reboot
        if reboot is False:
            logger.info("no need to reboot by global constant")
            return ok({"message": "no need to reboot by constant"})

        logger.debug("rebooting !!!!!!!!")
        try:
            sys.stdout.close_log()
        except Exception as e:
            logger.info(f"error reboot!!!!!!!! {e}")
            pass
        return os.execv(sys.executable, [sys.executable] + sys.argv)


    # must be sync invoke and use the env to check
    @server.PromptServer.instance.routes.post("/sync_instance")
    async def sync_instance(request):
        if not BUCKET:
            logger.error("No bucket provided ,wait and try again")
            resp = {"status": "success", "message": "syncing"}
            return ok(resp)

        if 'ALREADY_SYNC' in os.environ and os.environ.get('ALREADY_SYNC').lower() == 'false':
            resp = {"status": "success", "message": "syncing"}
            logger.error("other process doing ,wait and try again")
            return ok(resp)

        os.environ['ALREADY_SYNC'] = 'false'
        logger.info(f"sync_instance start ！！ {datetime.datetime.now().isoformat()} {request}")
        try:
            last_sync_record = get_last_ddb_sync_record()
            if not last_sync_record:
                logger.info("no last sync record found do not need sync")
                sync_instance_monitor_status(True)
                resp = {"status": "success", "message": "no sync"}
                os.environ['ALREADY_SYNC'] = 'true'
                return ok(resp)

            if ('request_id' in last_sync_record and last_sync_record['request_id']
                    and os.environ.get('LAST_SYNC_REQUEST_ID')
                    and os.environ.get('LAST_SYNC_REQUEST_ID') == last_sync_record['request_id']
                    and os.environ.get('LAST_SYNC_REQUEST_TIME')
                    and os.environ.get('LAST_SYNC_REQUEST_TIME') == str(last_sync_record['request_time'])):
                logger.info("last sync record already sync by os check")
                sync_instance_monitor_status(False)
                resp = {"status": "success", "message": "no sync env"}
                os.environ['ALREADY_SYNC'] = 'true'
                return ok(resp)

            instance_monitor_record = get_latest_ddb_instance_monitor_record()
            if not instance_monitor_record:
                sync_already = await prepare_comfy_env(last_sync_record)
                if sync_already:
                    logger.info("should init prepare instance_monitor_record")
                    sync_status = 'success' if sync_already else 'failed'
                    save_sync_instance_monitor(last_sync_record['request_id'], sync_status)
                else:
                    sync_instance_monitor_status(False)
            else:
                if ('last_sync_request_id' in instance_monitor_record
                        and instance_monitor_record['last_sync_request_id']
                        and instance_monitor_record['last_sync_request_id'] == last_sync_record['request_id']
                        and instance_monitor_record['sync_status']
                        and instance_monitor_record['sync_status'] == 'success'
                        and os.environ.get('LAST_SYNC_REQUEST_TIME')
                        and os.environ.get('LAST_SYNC_REQUEST_TIME') == str(last_sync_record['request_time'])):
                    logger.info("last sync record already sync")
                    sync_instance_monitor_status(False)
                    resp = {"status": "success", "message": "no sync ddb"}
                    os.environ['ALREADY_SYNC'] = 'true'
                    return ok(resp)

                sync_already = await prepare_comfy_env(last_sync_record)
                instance_monitor_record['sync_status'] = 'success' if sync_already else 'failed'
                instance_monitor_record['last_sync_request_id'] = last_sync_record['request_id']
                sync_list = instance_monitor_record['sync_list'] if ('sync_list' in instance_monitor_record
                                                                     and instance_monitor_record['sync_list']) else []
                sync_list.append(last_sync_record['request_id'])

                instance_monitor_record['sync_list'] = sync_list
                logger.info("should update prepare instance_monitor_record")
                update_sync_instance_monitor(instance_monitor_record)
            os.environ['ALREADY_SYNC'] = 'true'
            resp = {"status": "success", "message": "sync"}
            return ok(resp)
        except Exception as e:
            logger.info("exception occurred", e)
            os.environ['ALREADY_SYNC'] = 'true'
            resp = {"status": "fail", "message": "sync"}
            return error(resp)


    def validate_prompt_proxy(func):
        def wrapper(*args, **kwargs):
            logger.info("validate_prompt_proxy start...")
            result = func(*args, **kwargs)
            logger.info("validate_prompt_proxy end...")
            return result

        return wrapper


    execution.validate_prompt = validate_prompt_proxy(execution.validate_prompt)


    def send_sync_proxy(func):
        def wrapper(*args, **kwargs):
            logger.debug(f"Sending sync request!!!!!!! {args}")
            global need_sync
            global prompt_id
            logger.info(f"send_sync_proxy start... {need_sync},{prompt_id} {args}")
            func(*args, **kwargs)
            if need_sync and QUEUE_URL and REGION:
                logger.debug(f"send_sync_proxy params... {QUEUE_URL},{REGION},{need_sync},{prompt_id}")
                event = args[1]
                data = args[2]
                sid = args[3] if len(args) == 4 else None
                message_body = {'prompt_id': prompt_id, 'event': event, 'data': data, 'sid': sid}
                message_id = sen_sqs_msg(message_body, prompt_id)
                logger.info(f'send_sync_proxy message_id :{message_id} message_body: {message_body}')
            logger.debug(f"send_sync_proxy end...")

        return wrapper


    server.PromptServer.send_sync = send_sync_proxy(server.PromptServer.send_sync)


    def get_save_imge_path_proxy(func):
        def wrapper(*args, **kwargs):
            logger.info(f"get_save_imge_path_proxy args : {args} kwargs : {kwargs}")
            full_output_folder, filename, counter, subfolder, filename_prefix = func(*args, **kwargs)
            global prompt_id
            filename_prefix_new = filename_prefix + "_" + str(prompt_id)
            logger.info(f"get_save_imge_path_proxy filename_prefix new : {filename_prefix_new}")
            return full_output_folder, filename, counter, subfolder, filename_prefix_new

        return wrapper


    folder_paths.get_save_image_path = get_save_imge_path_proxy(folder_paths.get_save_image_path)
