#!/bin/bash

echo "---------------------------------------------------------------------------------"
echo "install comfy..."

export INITIAL_COMFY_COMMIT_ROOT=e6482fbbfc83cd25add0532b2e4c51d305e8a232

rm -rf ComfyUI
rm -rf stable-diffusion-aws-extension

git clone https://github.com/comfyanonymous/ComfyUI.git
cd ComfyUI || exit 1
git reset --hard ${INITIAL_COMFY_COMMIT_ROOT}
cd ../

git clone https://github.com/awslabs/stable-diffusion-aws-extension.git --branch "dev" --single-branch
if [ -n "$ESD_COMMIT_ID" ]; then
  cd stable-diffusion-aws-extension || exit 1
  echo "reset to ESD_COMMIT_ID: $ESD_COMMIT_ID"
  git reset --hard "$ESD_COMMIT_ID"
  cd ../
fi

cp stable-diffusion-aws-extension/build_scripts/comfy/serve.py ComfyUI/
cp stable-diffusion-aws-extension/build_scripts/comfy/comfy_sagemaker_proxy.py ComfyUI/custom_nodes/
cp stable-diffusion-aws-extension/build_scripts/comfy/comfy_local_proxy.py ComfyUI/custom_nodes/
cp -R stable-diffusion-aws-extension/build_scripts/comfy/ComfyUI-AWS-Extension ComfyUI/custom_nodes/ComfyUI-AWS-Extension

rm -rf stable-diffusion-aws-extension

echo "---------------------------------------------------------------------------------"
echo "build comfy..."

cd ComfyUI || exit 1

if [ "$ON_DOCKER" == "true" ]; then
  python3 -m venv venv
  source venv/bin/activate
fi

venv/bin/pyhon3 -m pip install --upgrade pip
venv/bin/pyhon3 -m pip install -r requirements.txt
venv/bin/pyhon3 -m pip install boto3
venv/bin/pyhon3 -m pip install aws_xray_sdk
venv/bin/pyhon3 -m pip install fastapi
venv/bin/pyhon3 -m pip install uvicorn
venv/bin/pyhon3 -m pip install watchdog
venv/bin/pyhon3 -m pip install python-dotenv
venv/bin/pyhon3 -m pip install httpx

if [ "$ON_DOCKER" == "true" ]; then
  venv/bin/pyhon3 -m pip install torch==2.0.1 torchvision==0.15.2 --extra-index-url https://download.pytorch.org/whl/cu118
  venv/bin/pyhon3 -m pip install https://github.com/openai/CLIP/archive/d50d76daa670286dd6cacf3bcd80b5e4823fc8e1.zip
  venv/bin/pyhon3 -m pip install https://github.com/mlfoundations/open_clip/archive/bb6e834e9c70d9c27d0dc3ecedeebeaeb1ffad6b.zip
  venv/bin/pyhon3 -m pip install open-clip-torch==2.20.0
fi
