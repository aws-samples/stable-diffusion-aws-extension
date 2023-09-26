from queue import Queue
import importlib
import logging
import gradio as gr
import os

import utils
from aws_extension.cloud_infer_service.simple_sagemaker_infer import SimpleSagemakerInfer
import modules.scripts as scripts
from aws_extension.sagemaker_ui import None_Option_For_On_Cloud_Model
from dreambooth_on_cloud.ui import ui_tabs_callback
from modules import script_callbacks, sd_models, processing, extra_networks, shared
from modules.api.models import StableDiffusionTxt2ImgProcessingAPI, StableDiffusionImg2ImgProcessingAPI
from modules.sd_hijack import model_hijack
from modules.processing import Processed
from modules.shared import cmd_opts
from aws_extension import sagemaker_ui

from aws_extension.cloud_models_manager.sd_manager import CloudSDModelsManager, postfix
from aws_extension.inference_scripts_helper.scripts_processor import process_args_by_plugin
from aws_extension.sagemaker_ui_tab import on_ui_tabs
from aws_extension.sagemaker_ui_utils import on_after_component_callback

dreambooth_available = True
logger = logging.getLogger(__name__)
logger.setLevel(utils.LOGGING_LEVEL)


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
    logging.warning(
        "[main]dreambooth_on_cloud is not installed or can not be imported, using dummy function to proceed.")
    dreambooth_available = False
    cloud_train = dummy_function
    get_cloud_db_model_name_list = dummy_function
    wrap_load_model_params = dummy_function
    get_train_job_list = dummy_function
    get_sorted_cloud_dataset = dummy_function
    get_sd_cloud_models = dummy_function
    get_create_model_job_list = dummy_function
    cloud_create_model = dummy_function


class SageMakerUI(scripts.Script):
    latest_result = None
    current_inference_id = None
    inference_queue = Queue(maxsize=30)
    default_images_inner = None
    txt2img_generate_btn = None
    img2img_generate_btn = None
    sd_model_manager = CloudSDModelsManager()
    infer_manager = SimpleSagemakerInfer()

    refresh_sd_model_checkpoint_btn = None
    setting_sd_model_checkpoint_dropdown = None

    txt2img_generation_info = None
    txt2img_gallery = None
    txt2img_html_info = None

    img2img_generation_info = None
    img2img_gallery = None
    img2img_html_info = None

    ph = None

    def title(self):
        return "SageMaker embeddings"

    def show(self, is_img2img):
        return scripts.AlwaysVisible

    def after_component(self, component, **kwargs):
        if type(component) is gr.Button:
            if self.is_txt2img and getattr(component, 'elem_id', None) == f'txt2img_generate':
                self.txt2img_generate_btn = component
            elif self.is_img2img and getattr(component, 'elem_id', None) == f'img2img_generate':
                self.img2img_generate_btn = component

        if type(component) is gr.Textbox and getattr(component, 'elem_id',
                                                     None) == 'generation_info_txt2img' and self.is_txt2img:
            self.txt2img_generation_info = component

        if type(component) is gr.Gallery and getattr(component, 'elem_id',
                                                     None) == 'txt2img_gallery' and self.is_txt2img:
            self.txt2img_gallery = component

        if type(component) is gr.HTML and getattr(component, 'elem_id',
                                                  None) == 'html_info_txt2img' and self.is_txt2img:
            self.txt2img_html_info = component

        async def _update_result():
            if self.inference_queue and not self.inference_queue.empty():
                inference_id = self.inference_queue.get()
                self.latest_result = sagemaker_ui.process_result_by_inference_id(inference_id)
                return self.latest_result

            return gr.skip(), gr.skip(), gr.skip()

        if self.txt2img_html_info and self.txt2img_gallery and self.txt2img_generation_info:
            self.txt2img_generation_info.change(
                fn=lambda: sagemaker_ui.async_loop_wrapper(_update_result),
                inputs=None,
                outputs=[self.txt2img_gallery, self.txt2img_generation_info, self.txt2img_html_info]
            )

        if type(component) is gr.Textbox and getattr(component, 'elem_id',
                                                     None) == 'generation_info_img2img' and self.is_img2img:
            self.img2img_generation_info = component

        if type(component) is gr.Gallery and getattr(component, 'elem_id',
                                                     None) == 'img2img_gallery' and self.is_img2img:
            self.img2img_gallery = component

        if type(component) is gr.HTML and getattr(component, 'elem_id',
                                                  None) == 'html_info_img2img' and self.is_img2img:
            self.img2img_html_info = component

        if self.img2img_html_info and self.img2img_gallery and self.img2img_generation_info:
            self.img2img_generation_info.change(
                fn=lambda: sagemaker_ui.async_loop_wrapper(_update_result),
                inputs=None,
                outputs=[self.img2img_gallery, self.img2img_generation_info, self.img2img_html_info]
            )

        pass

    def ui(self, is_img2img):
        def _check_generate(model_selected):
            return f'Generate{" on Cloud" if model_selected and model_selected != None_Option_For_On_Cloud_Model else ""}'

        if not is_img2img:
            model_on_cloud, inference_job_dropdown, primary_model_name, \
                secondary_model_name, tertiary_model_name, \
                modelmerger_merge_on_cloud = sagemaker_ui.create_ui(is_img2img)

            model_on_cloud.change(_check_generate, inputs=model_on_cloud,
                                  outputs=[self.txt2img_generate_btn])

            return [model_on_cloud, inference_job_dropdown,
                    primary_model_name, secondary_model_name, tertiary_model_name, modelmerger_merge_on_cloud]
        else:
            model_on_cloud, inference_job_dropdown, primary_model_name, secondary_model_name, tertiary_model_name, \
                modelmerger_merge_on_cloud = sagemaker_ui.create_ui(is_img2img)
            model_on_cloud.change(_check_generate, inputs=model_on_cloud,
                                  outputs=[self.img2img_generate_btn])
            return [model_on_cloud, inference_job_dropdown,
                    primary_model_name, secondary_model_name, tertiary_model_name, modelmerger_merge_on_cloud]

    def before_process(self, p, *args):
        on_docker = os.environ.get('ON_DOCKER', "false")
        if on_docker == "true":
            return

        # check if endpoint is inService
        sd_model_on_cloud = args[0]
        if sd_model_on_cloud == None_Option_For_On_Cloud_Model:
            return

        current_model = sd_models.select_checkpoint()
        logger.debug(current_model.name)
        models = {'Stable-diffusion': [sd_model_on_cloud]}

        api_param_cls = None

        if self.is_img2img:
            api_param_cls = StableDiffusionImg2ImgProcessingAPI

        if self.is_txt2img:
            api_param_cls = StableDiffusionTxt2ImgProcessingAPI

        if not api_param_cls:
            raise NotImplementedError

        p.sampler_index = p.sampler_name

        api_param = api_param_cls(**p.__dict__)
        if self.is_img2img:
            api_param.mask = p.image_mask

        selected_script_index = p.script_args[0] - 1
        selected_script_name = None if selected_script_index < 0 else p.scripts.selectable_scripts[selected_script_index].name
        api_param.script_args = []
        for sid, script in enumerate(p.scripts.scripts):
            # escape sagemaker plugin
            if script.title() == self.title():
                continue

            all_used_models = []
            script_args = p.script_args[script.args_from:script.args_to]
            if script.alwayson:
                logger.debug(f'{script.name} {script.args_from} {script.args_to}')
                api_param.alwayson_scripts[script.name] = {}
                api_param.alwayson_scripts[script.name]['args'] = []
                for _id, arg in enumerate(script_args):
                    parsed_args, used_models = process_args_by_plugin(script.name, arg, _id, script_args)
                    all_used_models.append(used_models)
                    api_param.alwayson_scripts[script.name]['args'].append(parsed_args)
            elif selected_script_name == script.name:
                api_param.script_name = script.name
                for _id, arg in enumerate(script_args):
                    parsed_args, used_models = process_args_by_plugin(script.name, arg, _id, script_args)
                    all_used_models.append(used_models)
                    api_param.script_args.append(parsed_args)

            if all_used_models:
                for used_models in all_used_models:
                    for key, vals in used_models.items():
                        if key not in models:
                            models[key] = []
                        for val in vals:
                            if val not in models[key]:
                                models[key].append(val)


        # fixme: not handle batches yet
        # we not support automatic for simplicity because the default is Automatic
        # if user need, has to select a vae model manually in the setting page
        if shared.opts.sd_vae and shared.opts.sd_vae not in ['None', 'Automatic']:
            models['VAE'] = [shared.opts.sd_vae]

        from modules.processing import get_fixed_seed

        seed = get_fixed_seed(p.seed)
        subseed = get_fixed_seed(p.subseed)
        p.setup_prompts()

        if type(seed) == list:
            p.all_seeds = seed
        else:
            p.all_seeds = [int(seed) + (x if p.subseed_strength == 0 else 0) for x in range(len(p.all_prompts))]

        if type(subseed) == list:
            p.all_subseeds = subseed
        else:
            p.all_subseeds = [int(subseed) + x for x in range(len(p.all_prompts))]

        p.init(p.all_prompts, p.all_seeds, p.all_subseeds)
        p.prompts = p.all_prompts
        p.negative_prompts = p.all_negative_prompts
        p.seeds = p.all_seeds
        p.subseeds = p.all_subseeds
        _prompts, extra_network_data = extra_networks.parse_prompts(p.all_prompts)

        # load lora
        for key, vals in extra_network_data.items():
            if key == 'lora':
                for val in vals:
                    if 'Lora' not in models:
                        models['Lora'] = []

                    lora_filename = val.positional[0]
                    lora_models_dir = os.path.join("models", "Lora")
                    for filename in os.listdir(lora_models_dir):
                        if filename.startswith(lora_filename):
                            if lora_filename not in models['Lora']:
                                models['Lora'].append(filename)
            if key == 'hypernet':
                logger.debug(key, vals)
                for val in vals:
                    if 'hypernetworks' not in models:
                        models['hypernetworks'] = []

                    hypernet_filename = shared.hypernetworks[val.positional[0]].split(os.path.sep)[-1]
                    if hypernet_filename not in models['hypernetworks']:
                        models['hypernetworks'].append(hypernet_filename)

        if os.path.exists(cmd_opts.embeddings_dir) and not p.do_not_reload_embeddings:
            model_hijack.embedding_db.load_textual_inversion_embeddings()

        p.setup_conds()

        # load all embedding models
        models['embeddings'] = [val.filename.split(os.path.sep)[-1] for val in
                                model_hijack.embedding_db.word_embeddings.values()]

        err = None
        try:
            inference_id = self.infer_manager.run(p.user, models, api_param, self.is_txt2img)
            self.current_inference_id = inference_id
            self.inference_queue.put(inference_id)
        except Exception as e:
            logger.error(e)
            err = str(e)

        def process_image_inner_hijack(processing_param):
            if not self.default_images_inner:
                default_processing = importlib.import_module("modules.processing")
                self.default_images_inner = default_processing.process_images_inner

            if self.default_images_inner:
                processing.process_images_inner = self.default_images_inner

            if err:
                return Processed(
                    p,
                    images_list=[],
                    seed=0,
                    info=f"Inference job is failed: { ', '.join(err) if isinstance(err, list) else err}",
                    subseed=0,
                    index_of_first_image=0,
                    infotexts=[],
                )

            processed = Processed(
                p,
                images_list=[],
                seed=0,
                info=f'Inference job with id {inference_id} has created and running on cloud now. Use Inference job in the SageMaker part to see the result.',
                subseed=0,
                index_of_first_image=0,
                infotexts=[],
            )

            return processed

        default_processing = importlib.import_module("modules.processing")
        self.default_images_inner = default_processing.process_images_inner
        processing.process_images_inner = process_image_inner_hijack

    def process(self, p, *args):
        pass


script_callbacks.on_after_component(on_after_component_callback)
script_callbacks.on_ui_tabs(on_ui_tabs)
script_callbacks.ui_tabs_callback = ui_tabs_callback

from aws_extension.auth_service.simple_cloud_auth import cloud_auth_manager

if cloud_auth_manager.enableAuth:
    cmd_opts.gradio_auth = cloud_auth_manager.create_config()
