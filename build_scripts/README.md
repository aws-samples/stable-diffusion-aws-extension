# Version control for stable-diffusion-aws-extension

Update time: 20231106

| Supported Framework/Extension | Version No.| Commit ID |
| --------------------- | --------- |  --------- |
| stable-diffusion-webui|1.6.1|4afaaf8a020c1df457bcf7250cb1c7f609699fa7|
| sd-webui-controlnet | v1.1.415|fce6775a6dddef52ecd658259e909687d9dedf72|
| sd_dreambooth_extension | 1.0.14|cf086c536b141fc522ff11f6cffc8b7b12da04b9|
# How to play with /stable-diffusion-webui

```
accelerate launch --num_cpu_threads_per_process=6 launch.py --api

```

# How to build images


### Build public images for aigc endpoint for byoc which is used for GPU operations, like txt2img inference.

```
sh build_and_push.sh Dockerfile.aigc-endpoint-byoc.from_scratch aigc-endpoint-byoc

```

### Build public images for aigc endpoint for diffusers which is used for GPU operations, like txt2img inference.

```
sh build_and_push.sh Dockerfile.aigc-endpoint-diffusers.from_scratch aigc-endpoint-diffusers

```

### Build public images for aigc endpoint which is used for training model

```
sh build_and_push.sh Dockerfile.aigc-job.from_scratch aigc-job

```

### Update public ecr to your private ecr

```
sh update_private_ecr.sh aigc-endpoint-byoc|aigc-endpoint-diffusers|aigc-job

```
