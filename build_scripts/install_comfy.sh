#!/bin/bash

# todo: deprecated

echo "---------------------------------------------------------------------------------"
echo "install comfy..."

export INITIAL_COMFY_COMMIT_ROOT=967867d48c29666630edfa10a1ab711217e3094e

rm -rf ComfyUI
rm -rf stable-diffusion-aws-extension

git clone https://github.com/comfyanonymous/ComfyUI.git
cd ComfyUI || exit 1
git reset --hard ${INITIAL_COMFY_COMMIT_ROOT}
cd ../

git clone https://github.com/awslabs/stable-diffusion-aws-extension.git --branch "dev"
if [ -n "$ESD_COMMIT_ID" ]; then
  cd stable-diffusion-aws-extension || exit 1
  echo "reset to ESD_COMMIT_ID: $ESD_COMMIT_ID"
  git reset --hard "$ESD_COMMIT_ID"
  cd ../
fi

cp stable-diffusion-aws-extension/build_scripts/comfy/serve.py ComfyUI/
cp stable-diffusion-aws-extension/build_scripts/comfy/comfy_proxy.py ComfyUI/custom_nodes/
#  TODO 6.14 delete
cp stable-diffusion-aws-extension/build_scripts/comfy/comfy_sagemaker_proxy.py ComfyUI/custom_nodes/
cp stable-diffusion-aws-extension/build_scripts/comfy/comfy_local_proxy.py ComfyUI/custom_nodes/
cp -R stable-diffusion-aws-extension/build_scripts/comfy/ComfyUI-AWS-Extension ComfyUI/custom_nodes/ComfyUI-AWS-Extension

rm -rf stable-diffusion-aws-extension

echo "---------------------------------------------------------------------------------"
echo "build comfy..."

cd ComfyUI || exit 1

git clone https://github.com/ltdrdata/ComfyUI-Manager.git custom_nodes/ComfyUI-Manager
git clone https://github.com/Gourieff/comfyui-reactor-node.git custom_nodes/comfyui-reactor-node
git clone https://github.com/twri/sdxl_prompt_styler.git  custom_nodes/sdxl_prompt_styler
git clone https://github.com/AIGODLIKE/AIGODLIKE-ComfyUI-Translation.git custom_nodes/AIGODLIKE-ComfyUI-Translation
git clone https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite.git custom_nodes/ComfyUI-VideoHelperSuite
git clone https://github.com/Kosinkadink/ComfyUI-AnimateDiff-Evolved.git custom_nodes/ComfyUI-AnimateDiff-Evolved

if [ "$ON_SAGEMAKER" == "true" ]; then
  python3 -m venv venv
  source venv/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
  pip install boto3
  pip install aws_xray_sdk
  pip install fastapi
  pip install uvicorn
  pip install watchdog
  pip install python-dotenv
  pip install httpx
  pip install onnxruntime

#  pip install torch==2.0.1 torchvision==0.15.2 --extra-index-url https://download.pytorch.org/whl/cu118
  pip install torch==2.4.0 torchvision==0.19.0 --extra-index-url https://download.pytorch.org/whl/cu118
  pip install https://github.com/openai/CLIP/archive/d50d76daa670286dd6cacf3bcd80b5e4823fc8e1.zip
  pip install https://github.com/mlfoundations/open_clip/archive/bb6e834e9c70d9c27d0dc3ecedeebeaeb1ffad6b.zip
  pip install open-clip-torch==2.20.0

  pip install -r custom_nodes/ComfyUI-Manager/requirements.txt
  pip install -r custom_nodes/comfyui-reactor-node/requirements.txt
  pip install -r custom_nodes/ComfyUI-VideoHelperSuite/requirements.txt
else
  #  ec2
  /venv/bin/python3 -m pip install --upgrade pip
  /venv/bin/python3 -m pip install -r requirements.txt
  /venv/bin/python3 -m pip install boto3
  /venv/bin/python3 -m pip install aws_xray_sdk
  /venv/bin/python3 -m pip install fastapi
  /venv/bin/python3 -m pip install uvicorn
  /venv/bin/python3 -m pip install watchdog
  /venv/bin/python3 -m pip install awscli
  /venv/bin/python3 -m pip install python-dotenv
  /venv/bin/python3 -m pip install httpx
  /venv/bin/python3 -s -m pip install onnxruntime

#  /venv/bin/python3 -m pip install torch==2.0.1 torchvision==0.15.2 --extra-index-url https://download.pytorch.org/whl/cu118
  /venv/bin/python3 -m pip install torch==2.4.0 torchvision==0.19.0 --extra-index-url https://download.pytorch.org/whl/cu118
  /venv/bin/python3 -m pip install https://github.com/openai/CLIP/archive/d50d76daa670286dd6cacf3bcd80b5e4823fc8e1.zip
  /venv/bin/python3 -m pip install https://github.com/mlfoundations/open_clip/archive/bb6e834e9c70d9c27d0dc3ecedeebeaeb1ffad6b.zip
  /venv/bin/python3 -m pip install open-clip-torch==2.20.0

  /venv/bin/python3 -s -m pip install -r custom_nodes/ComfyUI-Manager/requirements.txt
  /venv/bin/python3 -s -m pip install -r custom_nodes/comfyui-reactor-node/requirements.txt
  /venv/bin/python3 -s -m pip install -r custom_nodes/ComfyUI-VideoHelperSuite/requirements.txt
fi
