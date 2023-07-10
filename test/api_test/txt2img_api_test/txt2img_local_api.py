import json
import requests
import io
import base64
from PIL import Image, PngImagePlugin
import time
import os
from gradio.processing_utils import encode_pil_to_base64
import sys

sys.path.append("../../../middleware_api/lambda/inference")
from parse.parameter_parser import json_convert_to_payload
start_time = time.time()

url = "http://127.0.0.1:8082"

aigc_json_file = "../json_files/txt2img_test.json"
f = open(aigc_json_file)
aigc_params = json.load(f)
checkpoint_info = {'Stable-diffusion': {'v2-1_768-ema-pruned.safetensors': 's3://stable-diffusion-aws-extension-aigcbucketa457cb49-xfyck6nj4vlo/Stable-diffusion/checkpoint/custom/13896019-1ba4-478a-a5ec-b7e143e840ca/v2-1_768-ema-pruned.safetensors', 'meinamix_meinaV10.safetensors': 's3://stable-diffusion-aws-extension-aigcbucketa457cb49-xfyck6nj4vlo/Stable-diffusion/checkpoint/custom/491803b4-8293-4604-b879-7b1d3fa8f1df/meinamix_meinaV10.safetensors', 'cheeseDaddys_41.safetensors': 's3://stable-diffusion-aws-extension-aigcbucketa457cb49-xfyck6nj4vlo/Stable-diffusion/checkpoint/custom/6fc2a447-a2d6-427c-b520-fef0f4c5ce85/cheeseDaddys_41.safetensors', 'AnythingV5Ink_ink.safetensors': 's3://stable-diffusion-aws-extension-aigcbucketa457cb49-xfyck6nj4vlo/Stable-diffusion/checkpoint/custom/1a0227fc-5bb0-436b-aa87-80d487a536b3/AnythingV5Ink_ink.safetensors', 'camelliamix25DV2_v2.safetensors': 's3://stable-diffusion-aws-extension-aigcbucketa457cb49-xfyck6nj4vlo/Stable-diffusion/checkpoint/custom/2f5063ee-e2ac-40be-b48e-8762dfdc25eb/camelliamix25DV2_v2.safetensors', 'sd-v1-5-inpainting.ckpt': 's3://stable-diffusion-aws-extension-aigcbucketa457cb49-xfyck6nj4vlo/Stable-diffusion/checkpoint/custom/822a6754-87e7-495b-b71a-543cf78cefb2/sd-v1-5-inpainting.ckpt', 'yangk-style_2160_lora.safetensors': 's3://stable-diffusion-aws-extension-aigcbucketa457cb49-xfyck6nj4vlo/Stable-diffusion/checkpoint/custom/7a8ad4b0-0159-4c0d-a5b9-a6692f90902a/yangk-style_2160_lora.safetensors'}, 'embeddings': {}, 'Lora': {}, 'hypernetworks': {}, 'ControlNet': {'control_v11p_sd15_canny.pth': 's3://stable-diffusion-aws-extension-aigcbucketa457cb49-xfyck6nj4vlo/ControlNet/checkpoint/custom/a20edd04-535c-4d85-842b-95c3c743d819/control_v11p_sd15_canny.pth'}, 'sagemaker_endpoint': 'infer-endpoint-5d9775d', 'task_type': 'txt2img'}

task_type = 'txt2img'
print(f"Task Type: {task_type}")
payload = json_convert_to_payload(aigc_params, checkpoint_info, task_type)

model_list = []
model_list.append("v1-5-pruned-emaonly.safetensors")
model_list.append("yangk-style_2160_lora.safetensors")
model_list.append("sd-v1-5-inpainting.ckpt")
model_list.append("meinamix_meinaV10.safetensors")
model_list.append("cheeseDaddys_41.safetensors")
model_list.append("camelliamix25DV2_v2.safetensors")
model_list.append("AnythingV5Ink_ink.safetensors")
model_list.append("v2-1_768-ema-pruned.safetensors")
model_list.append("v1-5-pruned-emaonly.safetensors")
model_list.append("yangk-style_2160_lora.safetensors")
model_list.append("sd-v1-5-inpainting.ckpt")
model_list.append("meinamix_meinaV10.safetensors")
model_list.append("cheeseDaddys_41.safetensors")
model_list.append("camelliamix25DV2_v2.safetensors")
model_list.append("AnythingV5Ink_ink.safetensors")
model_list.append("v2-1_768-ema-pruned.safetensors")
model_list.append("v1-5-pruned-emaonly.safetensors")
model_list.append("yangk-style_2160_lora.safetensors")
model_list.append("sd-v1-5-inpainting.ckpt")
model_list.append("meinamix_meinaV10.safetensors")
model_list.append("cheeseDaddys_41.safetensors")
model_list.append("camelliamix25DV2_v2.safetensors")
model_list.append("AnythingV5Ink_ink.safetensors")
model_list.append("v2-1_768-ema-pruned.safetensors")

import psutil
# import gc

for model in model_list:
    payload["models"]["Stable-diffusion"]= [model]
    response = requests.post(url=f'{url}/invocations', json=payload)

    print(f'Model {model} RAM memory {psutil.virtual_memory()[2]} used: {psutil.virtual_memory()[3]/1000000000 } (GB)')

    # gc.collect()

# r = response.json()
# id = 0
# for i in r['images']:
#     image = Image.open(io.BytesIO(base64.b64decode(i.split(",",1)[0])))

#     png_payload = {
#         "image": "data:image/png;base64," + i
#     }
#     response2 = requests.post(url=f'{url}/sdapi/v1/png-info', json=png_payload)

#     pnginfo = PngImagePlugin.PngInfo()
#     pnginfo.add_text("parameters", response2.json().get("info"))
#     image.save('output_%d.png'%id, pnginfo=pnginfo)
#     id += 1
