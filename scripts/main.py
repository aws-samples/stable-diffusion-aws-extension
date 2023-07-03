import sys
import requests
import logging
import gradio as gr
import os
import modules.scripts as scripts
from modules import script_callbacks
from modules.ui import create_refresh_button
from modules.ui_components import FormRow
from utils import get_variable_from_json
from utils import save_variable_to_json
from PIL import Image

# sys.path.append("extensions/stable-diffusion-aws-extension/scripts")
# import sagemaker_ui
from aws_extension import sagemaker_ui

dreambooth_available = True
def dummy_function(*args, **kwargs):
    return []

try:
    from dreambooth_on_cloud.train import (
        async_cloud_train,
        get_cloud_db_model_name_list,
        wrap_load_model_params,
        get_train_job_list,
        get_sorted_cloud_dataset
    )
    from dreambooth_on_cloud.create_model import (
        get_sd_cloud_models,
        get_create_model_job_list,
        cloud_create_model,
    )
except Exception as e:
    logging.warning("[main]dreambooth_on_cloud is not installed or can not be imported, using dummy function to proceed.")
    dreambooth_available = False
    cloud_train = dummy_function
    get_cloud_db_model_name_list = dummy_function
    wrap_load_model_params = dummy_function
    get_train_job_list = dummy_function
    get_sorted_cloud_dataset = dummy_function
    get_sd_cloud_models = dummy_function
    get_create_model_job_list = dummy_function
    cloud_create_model = dummy_function

cloud_datasets = []
training_job_dashboard = None
db_model_name = None
cloud_db_model_name = None
cloud_train_instance_type = None
db_use_txt2img = None
db_sagemaker_train = None
db_save_config = None
txt2img_show_hook = None
txt2img_gallery = None
txt2img_generation_info = None
txt2img_html_info = None

img2img_show_hook = None
img2img_gallery = None
img2img_generation_info = None
img2img_html_info = None
modelmerger_merge_hook = None
modelmerger_merge_component = None

async_inference_choices=["ml.g4dn.xlarge","ml.g4dn.2xlarge","ml.g4dn.4xlarge","ml.g4dn.8xlarge","ml.g4dn.12xlarge", \
                         "ml.g5.xlarge","ml.g5.2xlarge","ml.g5.4xlarge","ml.g5.8xlarge","ml.g5.12xlarge"]

class SageMakerUI(scripts.Script):
    def title(self):
        return "SageMaker embeddings"

    def show(self, is_img2img):
        return scripts.AlwaysVisible

    def ui(self, is_img2img):
        if not is_img2img:
            sagemaker_endpoint, sd_checkpoint_txt2img, sd_checkpoint_refresh_button_txt2img, txt2img_textual_inversion_dropdown, txt2img_lora_dropdown, txt2img_hyperNetwork_dropdown, txt2img_controlnet_dropdown, inference_job_dropdown, txt2img_inference_job_ids_refresh_button, primary_model_name, secondary_model_name, tertiary_model_name, modelmerger_merge_on_cloud= sagemaker_ui.create_ui(is_img2img)
            return [sagemaker_endpoint, sd_checkpoint_txt2img, sd_checkpoint_refresh_button_txt2img,txt2img_textual_inversion_dropdown, txt2img_lora_dropdown, txt2img_hyperNetwork_dropdown, txt2img_controlnet_dropdown, inference_job_dropdown, txt2img_inference_job_ids_refresh_button, primary_model_name, secondary_model_name, tertiary_model_name, modelmerger_merge_on_cloud]
        else:
            sagemaker_endpoint, sd_checkpoint_img2img, sd_checkpoint_refresh_button_img2img, img2img_textual_inversion_dropdown, img2img_lora_dropdown, img2img_hyperNetwork_dropdown, img2img_controlnet_dropdown, inference_job_dropdown, txt2img_inference_job_ids_refresh_button, primary_model_name, secondary_model_name, tertiary_model_name, modelmerger_merge_on_cloud= sagemaker_ui.create_ui(is_img2img)
            return [sagemaker_endpoint, sd_checkpoint_img2img, sd_checkpoint_refresh_button_img2img, img2img_textual_inversion_dropdown, img2img_lora_dropdown, img2img_hyperNetwork_dropdown, img2img_controlnet_dropdown, inference_job_dropdown, txt2img_inference_job_ids_refresh_button, primary_model_name, secondary_model_name, tertiary_model_name, modelmerger_merge_on_cloud]

    def process(self, p, sagemaker_endpoint, sd_checkpoint_txt2img, sd_checkpoint_refresh_button_txt2img, sd_checkpoint_img2img,  sd_checkpoint_refresh_button_img2img,  textual_inversion_dropdown, lora_dropdown, hyperNetwork_dropdown, controlnet_dropdown, choose_txt2img_inference_job_id, txt2img_inference_job_ids_refresh_button, primary_model_name, secondary_model_name):
        pass

def on_after_component_callback(component, **_kwargs):
    global db_model_name, db_use_txt2img, db_sagemaker_train, db_save_config, cloud_db_model_name, cloud_train_instance_type, training_job_dashboard
    is_dreambooth_train = type(component) is gr.Button and getattr(component, 'elem_id', None) == 'db_train'
    is_dreambooth_model_name = type(component) is gr.Dropdown and \
                               (getattr(component, 'elem_id', None) == 'model_name' or \
                                (getattr(component, 'label', None) == 'Model' and getattr(component.parent.parent.parent.parent, 'elem_id', None) == 'ModelPanel'))
    is_cloud_dreambooth_model_name = type(component) is gr.Dropdown and \
                                     getattr(component, 'elem_id', None) == 'cloud_db_model_name'
    is_machine_type_for_train = type(component) is gr.Dropdown and \
                                getattr(component, 'elem_id', None) == 'cloud_train_instance_type'
    is_dreambooth_use_txt2img = type(component) is gr.Checkbox and getattr(component, 'label', None) == 'Use txt2img'
    is_training_job_dashboard = type(component) is gr.Dataframe and getattr(component, 'elem_id', None) == 'training_job_dashboard'
    is_db_save_config = getattr(component, 'elem_id', None) == 'db_save_config'
    if is_dreambooth_train:
        db_sagemaker_train = gr.Button(value="SageMaker Train", elem_id = "db_sagemaker_train", variant='primary')
    if is_dreambooth_model_name:
        db_model_name = component
    if is_cloud_dreambooth_model_name:
        cloud_db_model_name = component
    if is_training_job_dashboard:
        training_job_dashboard = component
    if is_machine_type_for_train:
        cloud_train_instance_type = component
    if is_dreambooth_use_txt2img:
        db_use_txt2img = component
    if is_db_save_config:
        db_save_config = component
    # After all requiment comment is loaded, add the SageMaker training button click callback function.
    if training_job_dashboard is not None and cloud_train_instance_type is not None and \
            cloud_db_model_name is not None and db_model_name is not None and \
            db_use_txt2img is not None and db_sagemaker_train is not None and \
            (is_dreambooth_train or is_dreambooth_model_name or is_dreambooth_use_txt2img or is_cloud_dreambooth_model_name or is_machine_type_for_train or is_training_job_dashboard):
        db_model_name.value = "dummy_local_model"
        db_sagemaker_train.click(
            fn=async_cloud_train,
            _js="db_start_sagemaker_train",
            inputs=[
                db_model_name,
                cloud_db_model_name,
                db_use_txt2img,
                cloud_train_instance_type
            ],
            outputs=[training_job_dashboard]
        )
    # Hook image display logic
    global txt2img_gallery, txt2img_generation_info, txt2img_html_info, txt2img_show_hook, txt2img_prompt
    is_txt2img_gallery = type(component) is gr.Gallery and getattr(component, 'elem_id', None) == 'txt2img_gallery'
    is_txt2img_generation_info = type(component) is gr.Textbox and getattr(component, 'elem_id', None) == 'generation_info_txt2img'
    is_txt2img_html_info = type(component) is gr.HTML and getattr(component, 'elem_id', None) == 'html_info_txt2img'
    is_txt2img_prompt = type(component) is gr.Textbox and getattr(component, 'elem_id', None) == 'txt2img_prompt'
    if is_txt2img_prompt:
        txt2img_prompt = component
    if is_txt2img_gallery:
        txt2img_gallery = component
    if is_txt2img_generation_info:
        txt2img_generation_info = component
    if is_txt2img_html_info:
        txt2img_html_info = component
        # return test
    if sagemaker_ui.inference_job_dropdown is not None and \
        txt2img_gallery is not None and \
        txt2img_generation_info is not None and \
        txt2img_html_info is not None and \
        txt2img_show_hook is None and \
        txt2img_prompt is not None:
        txt2img_show_hook = "finish"
        sagemaker_ui.inference_job_dropdown.change(
            fn=lambda selected_value: sagemaker_ui.fake_gan(selected_value),
            inputs=[sagemaker_ui.inference_job_dropdown],
            outputs=[txt2img_gallery, txt2img_generation_info, txt2img_html_info, txt2img_prompt]
        )

        sagemaker_ui.sagemaker_endpoint.change(
            fn=lambda selected_value: sagemaker_ui.displayEndpointInfo(selected_value),
            inputs=[sagemaker_ui.sagemaker_endpoint],
            outputs=[txt2img_html_info]
        )
        # elem_id = getattr(component, "elem_id", None)
        # if elem_id == "generate_on_cloud_with_cloud_config_button":
        sagemaker_ui.generate_on_cloud_button_with_js.click(
                fn=sagemaker_ui.call_txt2img_inference,
                _js="txt2img_config_save",
                inputs=[sagemaker_ui.sagemaker_endpoint],
                outputs=[txt2img_gallery, txt2img_generation_info, txt2img_html_info]
            )
        sagemaker_ui.modelmerger_merge_on_cloud.click(
            fn=sagemaker_ui.modelmerger_on_cloud_func,
            # fn=None,
            _js="txt2img_config_save",
            inputs=[sagemaker_ui.sagemaker_endpoint],
            # inputs=[
            #     sagemaker_ui.primary_model_name,
            #     sagemaker_ui.secondary_model_name,
            #     sagemaker_ui.tertiary_model_name,
            # ],
            outputs=[
            ])
        # Hook image display logic
    global img2img_gallery, img2img_generation_info, img2img_html_info, img2img_show_hook, \
            img2img_prompt, \
            init_img, \
            sketch, \
            init_img_with_mask, \
            inpaint_color_sketch, \
            init_img_inpaint, \
            init_mask_inpaint
    is_img2img_gallery = type(component) is gr.Gallery and getattr(component, 'elem_id', None) == 'img2img_gallery'
    is_img2img_generation_info = type(component) is gr.Textbox and getattr(component, 'elem_id', None) == 'generation_info_img2img'
    is_img2img_html_info = type(component) is gr.HTML and getattr(component, 'elem_id', None) == 'html_info_img2img'

    is_img2img_prompt = type(component) is gr.Textbox and getattr(component, 'elem_id', None) == 'img2img_prompt'
    is_init_img = type(component) is gr.Image and getattr(component, 'elem_id', None) == 'img2img_image'
    is_sketch = type(component) is gr.Image and getattr(component, 'elem_id', None) == 'img2img_sketch'
    is_init_img_with_mask = type(component) is gr.Image and getattr(component, 'elem_id', None) == 'img2maskimg'
    is_inpaint_color_sketch = type(component) is gr.Image and getattr(component, 'elem_id', None) == 'inpaint_sketch'


    is_init_img_inpaint = type(component) is gr.Image and getattr(component, 'elem_id', None) == 'img_inpaint_base'
    is_init_mask_inpaint = type(component) is gr.Image and getattr(component, 'elem_id', None) == 'img_inpaint_mask'

    if is_img2img_gallery:
        img2img_gallery = component
    if is_img2img_generation_info:
        img2img_generation_info = component
    if is_img2img_html_info:
        img2img_html_info = component

    if is_img2img_prompt:
        img2img_prompt = component
    if is_init_img:
        init_img = component
    if is_sketch:
        sketch = component
    if is_init_img_with_mask:
        init_img_with_mask = component
    if is_inpaint_color_sketch:
        inpaint_color_sketch = component
    if is_init_img_inpaint:
        init_img_inpaint = component
    if is_init_mask_inpaint:
        init_mask_inpaint = component

    if sagemaker_ui.inference_job_dropdown is not None and \
            img2img_gallery is not None and \
            img2img_generation_info is not None and \
            img2img_html_info is not None and \
            img2img_show_hook is None and \
            sagemaker_ui.interrogate_clip_on_cloud_button is not None and \
            sagemaker_ui.interrogate_deep_booru_on_cloud_button is not None and\
            img2img_prompt is not None and \
            init_img is not None and \
            sketch is not None and \
            init_img_with_mask is not None and \
            inpaint_color_sketch is not None and \
            init_img_inpaint is not None and \
            init_mask_inpaint is not None:
            img2img_show_hook = "finish"
            sagemaker_ui.inference_job_dropdown.change(
                fn=lambda selected_value: sagemaker_ui.fake_gan(selected_value),
                inputs=[sagemaker_ui.inference_job_dropdown],
                outputs=[img2img_gallery, img2img_generation_info, img2img_html_info, img2img_prompt]
                # outputs=[img2img_gallery, img2img_generation_info, img2img_html_info]
            )

            sagemaker_ui.interrogate_clip_on_cloud_button.click(
                fn=sagemaker_ui.call_interrogate_clip,
                _js="img2img_config_save",
                inputs=[sagemaker_ui.sagemaker_endpoint, init_img, sketch, init_img_with_mask, inpaint_color_sketch, init_img_inpaint, init_mask_inpaint],
                outputs=[img2img_gallery, img2img_generation_info, img2img_html_info]
            )

            sagemaker_ui.interrogate_deep_booru_on_cloud_button.click(
                fn=sagemaker_ui.call_interrogate_deepbooru,
                _js="img2img_config_save",
                inputs=[sagemaker_ui.sagemaker_endpoint, init_img, sketch, init_img_with_mask, inpaint_color_sketch, init_img_inpaint, init_mask_inpaint],
                outputs=[img2img_gallery, img2img_generation_info, img2img_html_info]
            )
            sagemaker_ui.generate_on_cloud_button_with_js_img2img.click(
                fn=sagemaker_ui.call_img2img_inference,
                _js="img2img_config_save",
                inputs=[sagemaker_ui.sagemaker_endpoint, init_img, sketch, init_img_with_mask, inpaint_color_sketch, init_img_inpaint, init_mask_inpaint],
                outputs=[img2img_gallery, img2img_generation_info, img2img_html_info]
            )

def update_connect_config(api_url, api_token):
    # Check if api_url ends with '/', if not append it
    if not api_url.endswith('/'):
        api_url += '/'

    save_variable_to_json('api_gateway_url', api_url)
    save_variable_to_json('api_token', api_token)
    global api_gateway_url
    api_gateway_url = get_variable_from_json('api_gateway_url')
    global api_key
    api_key = get_variable_from_json('api_token')
    print(f"update the api_url:{api_gateway_url} and token: {api_key}............")
    sagemaker_ui.init_refresh_resource_list_from_cloud()
    return "Setting updated"

def test_aws_connect_config(api_url, api_token):
    update_connect_config(api_url, api_token)
    api_url = get_variable_from_json('api_gateway_url')
    api_token = get_variable_from_json('api_token')
    if not api_url.endswith('/'):
        api_url += '/'
    print(f"get the api_url:{api_url} and token: {api_token}............")
    target_url = f'{api_url}inference/test-connection'
    headers = {
        "x-api-key": api_token,
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(target_url,headers=headers)  # Assuming sagemaker_ui.server_request is a wrapper around requests
        response.raise_for_status()  # Raise an exception if the HTTP request resulted in an error
        r = response.json()
        return "Successfully Connected"
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to get server request. Details: {e}")
        return "failed to connect to backend server, please check the url and token"

def on_ui_tabs():
    import modules.ui
    buildin_model_list = ['AWS JumpStart Model','AWS BedRock Model','Hugging Face Model']
    with gr.Blocks() as sagemaker_interface:
        with gr.Row(equal_height=True, elem_id="aws_sagemaker_ui_row", visible=False):
            sm_load_params = gr.Button(value="Load Settings", elem_id="aws_load_params", visible=False)
            sm_save_params = gr.Button(value="Save Settings", elem_id="aws_save_params", visible=False)
            sm_train_model = gr.Button(value="Train", variant="primary", elem_id="aws_train_model", visible=False)
            sm_generate_checkpoint = gr.Button(value="Generate Ckpt", elem_id="aws_gen_ckpt", visible=False)
        with gr.Row():
            gr.HTML(value="Enter your API URL & Token to start the connection.", elem_id="hint_row")
        with gr.Row():
            with gr.Column(variant="panel", scale=1):
                gr.HTML(value="<u><b>AWS Connection Setting</b></u>")
                global api_gateway_url
                api_gateway_url = get_variable_from_json('api_gateway_url')
                global api_key
                api_key = get_variable_from_json('api_token')
                with gr.Row():
                    api_url_textbox = gr.Textbox(value=api_gateway_url, lines=1, placeholder="Please enter API Url of Middle", label="API Url",elem_id="aws_middleware_api")
                    def update_api_gateway_url():
                        global api_gateway_url
                        api_gateway_url = get_variable_from_json('api_gateway_url')
                        return api_gateway_url
                    # modules.ui.create_refresh_button(api_url_textbox, get_variable_from_json('api_gateway_url'), lambda: {"value": get_variable_from_json('api_gateway_url')}, "refresh_api_gate_way")
                    modules.ui.create_refresh_button(api_url_textbox, update_api_gateway_url, lambda: {"value": api_gateway_url}, "refresh_api_gateway_url")
                with gr.Row():
                    def update_api_key():
                        global api_key
                        api_key = get_variable_from_json('api_token')
                        return api_key
                    api_token_textbox = gr.Textbox(value=api_key, lines=1, placeholder="Please enter API Token", label="API Token", elem_id="aws_middleware_token")
                    modules.ui.create_refresh_button(api_token_textbox, update_api_key, lambda: {"value": api_key}, "refresh_api_token")

                global test_connection_result
                test_connection_result = gr.Label(title="Output");
                aws_connect_button = gr.Button(value="Update Setting", variant='primary',elem_id="aws_config_save")
                aws_connect_button.click(_js="update_auth_settings",
                                         fn=update_connect_config,
                                         inputs = [api_url_textbox, api_token_textbox],
                                         outputs= [test_connection_result])
                aws_test_button = gr.Button(value="Test Connection", variant='primary',elem_id="aws_config_test")
                aws_test_button.click(test_aws_connect_config, inputs = [api_url_textbox, api_token_textbox], outputs=[test_connection_result])

                with gr.Row():
                    with gr.Accordion("Disclaimer", open=False):
                        gr.HTML(value="You should perform your own independent assessment, and take measures to ensure that you comply with your own specific quality control practices and standards, and the local rules, laws, regulations, licenses and terms of use that apply to you, your content, and the third-party generative AI service in this web UI. Amazon Web Services has no control or authority over the third-party generative AI service in this web UI, and does not make any representations or warranties that the third-party generative AI service is secure, virus-free, operational, or compatible with your production environment and standards.");

            with gr.Column(variant="panel", scale=1.5):
                gr.HTML(value="<u><b>Cloud Assets Management</b></u>")
                sagemaker_html_log = gr.HTML(elem_id=f'html_log_sagemaker')
                with gr.Accordion("Upload Model to S3", open=False):
                    gr.HTML(value="Refresh to select the model to upload to S3")
                    exts = (".bin", ".pt", ".pth", ".safetensors", ".ckpt")
                    root_path = os.getcwd()
                    model_folders = {
                        "ckpt": os.path.join(root_path, "models", "Stable-diffusion"),
                        "text": os.path.join(root_path, "embeddings"),
                        "lora": os.path.join(root_path, "models", "Lora"),
                        "control": os.path.join(root_path, "models", "ControlNet"),
                        "hyper": os.path.join(root_path, "models", "hypernetworks"),
                    }
                    def scan_sd_ckpt():
                        model_files = os.listdir(model_folders["ckpt"])
                        # filter non-model files not in exts
                        model_files = [f for f in model_files if os.path.splitext(f)[1] in exts]
                        model_files = [os.path.join(model_folders["ckpt"], f) for f in model_files]
                        return model_files
                    def scan_textural_inversion_model():
                        model_files = os.listdir(model_folders["text"])
                        # filter non-model files not in exts
                        model_files = [f for f in model_files if os.path.splitext(f)[1] in exts]
                        model_files = [os.path.join(model_folders["text"], f) for f in model_files]
                        return model_files
                    def scan_lora_model():
                        model_files = os.listdir(model_folders["lora"])
                        # filter non-model files not in exts
                        model_files = [f for f in model_files if os.path.splitext(f)[1] in exts]
                        model_files = [os.path.join(model_folders["lora"], f) for f in model_files]
                        return model_files
                    def scan_control_model():
                        model_files = os.listdir(model_folders["control"])
                        # filter non-model files not in exts
                        model_files = [f for f in model_files if os.path.splitext(f)[1] in exts]
                        model_files = [os.path.join(model_folders["control"], f) for f in model_files]
                        return model_files
                    def scan_hypernetwork_model():
                        model_files = os.listdir(model_folders["hyper"])
                        # filter non-model files not in exts
                        model_files = [f for f in model_files if os.path.splitext(f)[1] in exts]
                        model_files = [os.path.join(model_folders["hyper"], f) for f in model_files]
                        return model_files

                    with FormRow(elem_id="model_upload_form_row_01"):
                        sd_checkpoints_path = gr.Dropdown(label="SD Checkpoints", choices=sorted(scan_sd_ckpt()), elem_id="sd_ckpt_dropdown")
                        create_refresh_button(sd_checkpoints_path, scan_sd_ckpt, lambda: {"choices": sorted(scan_sd_ckpt())}, "refresh_sd_ckpt")

                        textual_inversion_path = gr.Dropdown(label="Textual Inversion", choices=sorted(scan_textural_inversion_model()),elem_id="textual_inversion_model_dropdown")
                        create_refresh_button(textual_inversion_path, scan_textural_inversion_model, lambda: {"choices": sorted(scan_textural_inversion_model())},  "refresh_textual_inversion_model")
                    with FormRow(elem_id="model_upload_form_row_02"):
                        lora_path = gr.Dropdown(label="LoRA model", choices=sorted(scan_lora_model()), elem_id="lora_model_dropdown")
                        create_refresh_button(lora_path, scan_lora_model, lambda: {"choices": sorted(scan_lora_model())}, "refresh_lora_model",)

                        controlnet_model_path = gr.Dropdown(label="ControlNet model", choices=sorted(scan_control_model()), elem_id="controlnet_model_dropdown")
                        create_refresh_button(controlnet_model_path, scan_control_model, lambda: {"choices": sorted(scan_control_model())}, "refresh_controlnet_models")
                    with FormRow(elem_id="model_upload_form_row_03"):
                        hypernetwork_path = gr.Dropdown(label="Hypernetwork", choices=sorted(scan_hypernetwork_model()),elem_id="hyper_model_dropdown")
                        create_refresh_button(hypernetwork_path, scan_hypernetwork_model, lambda: {"choices": sorted(scan_hypernetwork_model())}, "refresh_hyper_models")

                    with gr.Row():
                        model_update_button = gr.Button(value="Upload Models to Cloud", variant="primary",elem_id="sagemaker_model_update_button", size=(200, 50))
                        model_update_button.click(_js="model_update",
                                                  fn=sagemaker_ui.sagemaker_upload_model_s3,
                                                  inputs=[sd_checkpoints_path, textual_inversion_path, lora_path, hypernetwork_path, controlnet_model_path],
                                                  outputs=[test_connection_result, sd_checkpoints_path, textual_inversion_path, lora_path, hypernetwork_path, controlnet_model_path])


                with gr.Blocks(title="Deploy New SageMaker Endpoint", variant='panel'):
                    gr.HTML(value="<u><b>Deploy New SageMaker Endpoint</b></u>")
                    with gr.Row():
                        instance_type_dropdown = gr.Dropdown(label="SageMaker Instance Type", choices=async_inference_choices, elem_id="sagemaker_inference_instance_type_textbox", value="ml.g4dn.xlarge")
                        instance_count_dropdown = gr.Dropdown(label="Please select Instance count", choices=["1","2","3","4"], elem_id="sagemaker_inference_instance_count_textbox", value="1")

                    with gr.Row():
                        sagemaker_deploy_button = gr.Button(value="Deploy", variant='primary',elem_id="sagemaker_deploy_endpoint_buttion")
                        sagemaker_deploy_button.click(sagemaker_ui.sagemaker_deploy,
                                                      _js="deploy_endpoint", \
                                                      inputs = [instance_type_dropdown, instance_count_dropdown],
                                                      outputs=[test_connection_result])

                with gr.Blocks(title="Delete SageMaker Endpoint", variant='panel'):
                    gr.HTML(value="<u><b>Delete SageMaker Endpoint</b></u>")
                    with gr.Row():
                        sagemaker_endpoint_delete_dropdown = gr.Dropdown(choices=sagemaker_ui.sagemaker_endpoints, multiselect=True, label="Select Cloud SageMaker Endpoint")
                        modules.ui.create_refresh_button(sagemaker_endpoint_delete_dropdown, sagemaker_ui.update_sagemaker_endpoints, lambda: {"choices": sagemaker_ui.sagemaker_endpoints}, "refresh_sagemaker_endpoints_delete")
                    sagemaker_endpoint_delete_button = gr.Button(value="Delete", variant='primary',elem_id="sagemaker_endpoint_delete_button")
                    sagemaker_endpoint_delete_button.click(sagemaker_ui.sagemaker_endpoint_delete,
                                                           _js="delete_sagemaker_endpoint", \
                                                           inputs = [sagemaker_endpoint_delete_dropdown],
                                                           outputs=[test_connection_result])

            with gr.Column(variant="panel", scale=1):
                # TODO: uncomment if implemented, comment since the tab component do not has visible parameter
                # with gr.Blocks(title="Deploy New SageMaker Endpoint", variant='panel', visible=False):
                #     gr.HTML(value="<u><b>AWS Model Setting</b></u>", visible=False)
                #     with gr.Tab("Select"):
                #         gr.HTML(value="AWS Built-in Model", visible=False)
                #         model_select_dropdown = gr.Dropdown(buildin_model_list, label="Select Built-In Model", elem_id="aws_select_model", visible=False)
                #     with gr.Tab("Create"):
                #         gr.HTML(value="AWS Custom Model", visible=False)
                #         model_name_textbox = gr.Textbox(value="", lines=1, placeholder="Please enter model name", label="Model Name", visible=False)
                #         model_create_button = gr.Button(value="Create Model", variant='primary',elem_id="aws_create_model", visible=False)

                with gr.Blocks(title="Create AWS dataset", variant='panel'):
                    gr.HTML(value="<u><b>AWS Dataset Management</b></u>")
                    with gr.Tab("Create"):
                        def upload_file(files):
                            file_paths = [file.name for file in files]
                            return file_paths

                        file_output = gr.File()
                        upload_button = gr.UploadButton("Click to Upload a File", file_types=["image", "video"], file_count="multiple")
                        upload_button.upload(fn=upload_file, inputs=[upload_button], outputs=[file_output])

                        def create_dataset(files, dataset_name, dataset_desc):
                            print(dataset_name)
                            dataset_content = []
                            file_path_lookup = {}
                            for file in files:
                                orig_name = file.name.split(os.sep)[-1]
                                file_path_lookup[orig_name] = file.name
                                dataset_content.append(
                                    {
                                        "filename": orig_name,
                                        "name": orig_name,
                                        "type": "image",
                                        "params": {}
                                    }
                                )

                            payload = {
                                "dataset_name": dataset_name,
                                "content": dataset_content,
                                "params": {
                                    "description": dataset_desc
                                }
                            }

                            url = get_variable_from_json('api_gateway_url') + '/dataset'
                            api_key = get_variable_from_json('api_token')

                            raw_response = requests.post(url=url, json=payload, headers={'x-api-key': api_key})
                            raw_response.raise_for_status()
                            response = raw_response.json()

                            print(f"Start upload sample files response:\n{response}")
                            for filename, presign_url in response['s3PresignUrl'].items():
                                file_path = file_path_lookup[filename]
                                with open(file_path, 'rb') as f:
                                    response = requests.put(presign_url, f)
                                    print(response)
                                    response.raise_for_status()

                            payload = {
                                "dataset_name": dataset_name,
                                "status": "Enabled"
                            }

                            raw_response = requests.put(url=url, json=payload, headers={'x-api-key': api_key})
                            raw_response.raise_for_status()
                            print(raw_response.json())
                            return f'Complete Dataset {dataset_name} creation', None, None, None, None

                        dataset_name_upload = gr.Textbox(value="", lines=1, placeholder="Please input dataset name", label="Dataset Name",elem_id="sd_dataset_name_textbox")
                        dataset_description_upload = gr.Textbox(value="", lines=1, placeholder="Please input dataset description", label="Dataset Description",elem_id="sd_dataset_description_textbox")
                        create_dataset_button = gr.Button("Create Dataset", variant="primary", elem_id="sagemaker_dataset_create_button") # size=(200, 50)
                        dataset_create_result = gr.Textbox(value="", label="Create Result", interactive=False)
                        create_dataset_button.click(
                            fn=create_dataset,
                            inputs=[upload_button, dataset_name_upload, dataset_description_upload],
                            outputs=[
                                dataset_create_result,
                                dataset_name_upload,
                                dataset_description_upload,
                                file_output,
                                upload_button
                            ],
                            show_progress=True
                        )

                    with gr.Tab('Browse'):
                        with gr.Row():
                            global cloud_datasets
                            cloud_datasets = get_sorted_cloud_dataset()

                            cloud_dataset_name = gr.Dropdown(
                                label="Dataset From Cloud",
                                choices=[d['datasetName'] for d in cloud_datasets],
                                elem_id="cloud_dataset_dropdown",
                                type="index",
                                info='select datasets from cloud'
                            )

                            def refresh_datasets():
                                global cloud_datasets
                                cloud_datasets = get_sorted_cloud_dataset()
                                return cloud_datasets

                            def refresh_datasets_dropdown():
                                global cloud_datasets
                                cloud_datasets = get_sorted_cloud_dataset()
                                return {"choices": [d['datasetName'] for d in cloud_datasets]}

                            create_refresh_button(
                                cloud_dataset_name,
                                refresh_datasets,
                                refresh_datasets_dropdown,
                                "refresh_cloud_dataset",
                            )
                        with gr.Row():
                            dataset_s3_output = gr.Textbox(label='dataset s3 location', show_label=True, type='text').style(show_copy_button=True)
                        with gr.Row():
                            dataset_des_output = gr.Textbox(label='dataset description', show_label=True, type='text')
                        with gr.Row():
                            dataset_gallery = gr.Gallery(
                                label="Dataset images", show_label=False, elem_id="gallery",
                            ).style(columns=[2], rows=[2], object_fit="contain", height="auto")

                            def get_results_from_datasets(dataset_idx):
                                ds = cloud_datasets[dataset_idx]

                                url = f"{get_variable_from_json('api_gateway_url')}/dataset/{ds['datasetName']}/data"
                                api_key = get_variable_from_json('api_token')
                                raw_response = requests.get(url=url, headers={'x-api-key': api_key})
                                raw_response.raise_for_status()
                                dataset_items = [ (Image.open(requests.get(item['preview_url'], stream=True).raw), item['key']) for item in raw_response.json()['data']]
                                return ds['s3'], ds['description'], dataset_items

                            cloud_dataset_name.select(fn=get_results_from_datasets, inputs=[cloud_dataset_name], outputs=[dataset_s3_output, dataset_des_output, dataset_gallery])



    return (sagemaker_interface, "Amazon SageMaker", "sagemaker_interface"),


script_callbacks.on_after_component(on_after_component_callback)
script_callbacks.on_ui_tabs(on_ui_tabs)
# create new tabs for create Model
origin_callback = script_callbacks.ui_tabs_callback

def avoid_duplicate_from_restart_ui(res):
    for extension_ui in res:
        if extension_ui[1] == 'Dreambooth':
            for key in list(extension_ui[0].blocks):
                val = extension_ui[0].blocks[key]
                if type(val) is gr.Tab:
                    if val.label == 'Select From Cloud':
                        return True

    return False



def ui_tabs_callback():
    res = origin_callback()
    if avoid_duplicate_from_restart_ui(res):
        return res
    for extension_ui in res:
        if extension_ui[1] == 'Dreambooth':
            for key in list(extension_ui[0].blocks):
                val = extension_ui[0].blocks[key]
                if type(val) is gr.Tab:
                    if val.label == 'Select':
                        with extension_ui[0]:
                            with val.parent:
                                with gr.Tab('Select From Cloud'):
                                    with gr.Row():
                                        cloud_db_model_name = gr.Dropdown(
                                            label="Model", choices=sorted(get_cloud_db_model_name_list()),
                                            elem_id="cloud_db_model_name"
                                        )
                                        create_refresh_button(
                                            cloud_db_model_name,
                                            get_cloud_db_model_name_list,
                                            lambda: {"choices": sorted(get_cloud_db_model_name_list())},
                                            "refresh_db_models",
                                        )
                                    with gr.Row():
                                        cloud_db_snapshot = gr.Dropdown(
                                            label="Cloud Snapshot to Resume",
                                            choices=sorted(get_cloud_model_snapshots()),
                                            elem_id="cloud_snapshot_to_resume_dropdown"
                                        )
                                        create_refresh_button(
                                            cloud_db_snapshot,
                                            get_cloud_model_snapshots,
                                            lambda: {"choices": sorted(get_cloud_model_snapshots())},
                                            "refresh_db_snapshots",
                                        )

                                    with gr.Row():
                                        cloud_train_instance_type = gr.Dropdown(
                                            label="SageMaker Train Instance Type",
                                            choices=['ml.g4dn.2xlarge', 'ml.g5.2xlarge'],
                                            elem_id="cloud_train_instance_type",
                                            info='select SageMaker Train Instance Type'
                                        )
                                    with gr.Row(visible=False) as lora_model_row:
                                        cloud_db_lora_model_name = gr.Dropdown(
                                            label="Lora Model", choices=get_sorted_lora_cloud_models(),
                                            elem_id="cloud_lora_model_dropdown"
                                        )
                                        create_refresh_button(
                                            cloud_db_lora_model_name,
                                            get_sorted_lora_cloud_models,
                                            lambda: {"choices": get_sorted_lora_cloud_models()},
                                            "refresh_lora_models",
                                        )
                                    with gr.Row():
                                        gr.HTML(value="Loaded Model from Cloud:")
                                        cloud_db_model_path = gr.HTML()
                                    with gr.Row():
                                        gr.HTML(value="Cloud Model Revision:")
                                        cloud_db_revision = gr.HTML(elem_id="cloud_db_revision")
                                    with gr.Row():
                                        gr.HTML(value="Cloud Model Epoch:")
                                        cloud_db_epochs = gr.HTML(elem_id="cloud_db_epochs")
                                    with gr.Row():
                                        gr.HTML(value="V2 Model From Cloud:")
                                        cloud_db_v2 = gr.HTML(elem_id="cloud_db_v2")
                                    with gr.Row():
                                        gr.HTML(value="Has EMA:")
                                        cloud_db_has_ema = gr.HTML(elem_id="cloud_db_has_ema")
                                    with gr.Row():
                                        gr.HTML(value="Source Checkpoint From Cloud:")
                                        cloud_db_src = gr.HTML()
                                    with gr.Row():
                                        gr.HTML(value="Cloud DB Status:")
                                        cloud_db_status = gr.HTML(elem_id="db_status", value="")
                                    with gr.Row():
                                        gr.HTML(value="Experimental Shared Source:")
                                        cloud_db_shared_diffusers_path = gr.HTML()
                                    with gr.Row():
                                        gr.HTML(value="<b>Training Jobs Details:<b/>")
                                    with gr.Row():
                                        training_job_dashboard = gr.Dataframe(
                                            headers=["id", "model name", "status", "SageMaker train name"],
                                            datatype=["str", "str", "str", "str"],
                                            col_count=(4, "fixed"),
                                            value=get_train_job_list,
                                            interactive=False,
                                            every=3,
                                            elem_id='training_job_dashboard'
                                            # show_progress=True
                                        )
                                with gr.Tab('Create From Cloud'):
                                    with gr.Column():
                                        cloud_db_create_model = gr.Button(
                                            value="Create Model From Cloud", variant="primary"
                                        )
                                    cloud_db_new_model_name = gr.Textbox(label="Name", placeholder="Model names can only contain alphanumeric and -")
                                    with gr.Row():
                                        cloud_db_create_from_hub = gr.Checkbox(
                                            label="Create From Hub", value=False, visible=False
                                        )
                                        cloud_db_512_model = gr.Checkbox(label="512x Model", value=True)
                                    with gr.Column(visible=False) as hub_row:
                                        cloud_db_new_model_url = gr.Textbox(
                                            label="Model Path",
                                            placeholder="runwayml/stable-diffusion-v1-5",
                                            elem_id="cloud_db_model_path_text_box"
                                        )
                                        cloud_db_new_model_token = gr.Textbox(
                                            label="HuggingFace Token", value=""
                                        )
                                    with gr.Column(visible=True) as local_row:
                                        with gr.Row():
                                            cloud_db_new_model_src = gr.Dropdown(
                                                label="Source Checkpoint",
                                                choices=sorted(get_sd_cloud_models()),
                                                elem_id="cloud_db_source_checkpoint_dropdown"
                                            )
                                            create_refresh_button(
                                                cloud_db_new_model_src,
                                                get_sd_cloud_models,
                                                lambda: {"choices": sorted(get_sd_cloud_models())},
                                                "refresh_sd_models",
                                            )
                                    with gr.Column(visible=False) as shared_row:
                                        with gr.Row():
                                            cloud_db_new_model_shared_src = gr.Dropdown(
                                                label="EXPERIMENTAL: LoRA Shared Diffusers Source",
                                                choices=[],
                                                value=""
                                            )
                                    cloud_db_new_model_extract_ema = gr.Checkbox(
                                        label="Extract EMA Weights", value=False
                                    )
                                    cloud_db_train_unfrozen = gr.Checkbox(label="Unfreeze Model", value=False, elem_id="cloud_db_unfreeze_model_checkbox")
                                    with gr.Row():
                                        gr.HTML(value="<b>Model Creation Jobs Details:<b/>")
                                    with gr.Row():
                                        createmodel_dashboard = gr.Dataframe(
                                            headers=["id", "model name", "status"],
                                            datatype=["str", "str", "str"],
                                            col_count=(3, "fixed"),
                                            value=get_create_model_job_list,
                                            interactive=False,
                                            every=3
                                            # show_progress=True
                                        )

                                def toggle_new_rows(create_from):
                                    return gr.update(visible=create_from), gr.update(visible=not create_from)

                                cloud_db_create_from_hub.change(
                                    fn=toggle_new_rows,
                                    inputs=[cloud_db_create_from_hub],
                                    outputs=[hub_row, local_row],
                                )

                                cloud_db_model_name.change(
                                    _js="clear_loaded",
                                    fn=wrap_load_model_params,
                                    inputs=[cloud_db_model_name],
                                    outputs=[
                                        cloud_db_model_path,
                                        cloud_db_revision,
                                        cloud_db_epochs,
                                        cloud_db_v2,
                                        cloud_db_has_ema,
                                        cloud_db_src,
                                        cloud_db_shared_diffusers_path,
                                        cloud_db_snapshot,
                                        cloud_db_lora_model_name,
                                        cloud_db_status,
                                    ],
                                )
                                cloud_db_create_model.click(
                                    fn=cloud_create_model,
                                    _js="check_create_model_params",
                                    inputs=[
                                        cloud_db_new_model_name,
                                        cloud_db_new_model_src,
                                        cloud_db_new_model_shared_src,
                                        cloud_db_create_from_hub,
                                        cloud_db_new_model_url,
                                        cloud_db_new_model_token,
                                        cloud_db_new_model_extract_ema,
                                        cloud_db_train_unfrozen,
                                        cloud_db_512_model,
                                    ],
                                    outputs=[
                                        createmodel_dashboard
                                        # cloud_db_new_model_name
                                        # cloud_db_create_from_hub
                                        # cloud_db_512_model
                                        # cloud_db_new_model_url
                                        # cloud_db_new_model_token
                                        # cloud_db_new_model_src
                                    ]
                                )
                    break
    return res

script_callbacks.ui_tabs_callback = ui_tabs_callback

def get_sorted_lora_cloud_models():
    return []

def get_cloud_model_snapshots():
    return []