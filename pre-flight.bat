@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

set INITIAL_SUPPORT_COMMIT_ROOT=4afaaf8a020c1df457bcf7250cb1c7f609699fa7
set INITIAL_SUPPORT_COMMIT_CONTROLNET=fce6775a6dddef52ecd658259e909687d9dedf72
set INITIAL_SUPPORT_COMMIT_DREAMBOOTH=c2a5617c587b812b5a408143ddfb18fc49234edf
set INITIAL_SUPPORT_COMMIT_REMBG=3d9eedbbf0d585207f97d5b21e42f32c0042df70
set INITIAL_SUPPORT_COMMIT_SAM=d80220ecd2151cebfb00a5829be8ec1c05118000
set INITIAL_SUPPORT_COMMIT_TILEDVAE=fbb24736c9bc374c7f098f82b575fcd14a73936a

set REPO_URL_LIST[0]="https://github.com/Mikubill/sd-webui-controlnet.git"
set REPO_URL_LIST[1]="https://github.com/d8ahazard/sd_dreambooth_extension.git"
set REPO_URL_LIST[2]="https://github.com/AUTOMATIC1111/stable-diffusion-webui-rembg.git"
set REPO_URL_LIST[3]="https://github.com/continue-revolution/sd-webui-segment-anything.git"
set REPO_URL_LIST[4]="https://github.com/pkuliyi2015/multidiffusion-upscaler-for-automatic1111.git"

set REPO_FOLDER_LIST[0]="sd-webui-controlnet"
set REPO_FOLDER_LIST[1]="sd_dreambooth_extension"
set REPO_FOLDER_LIST[2]="stable-diffusion-webui-rembg"
set REPO_FOLDER_LIST[3]="sd-webui-segment-anything"
set REPO_FOLDER_LIST[4]="multidiffusion-upscaler-for-automatic1111"


call :parse_options %*
EXIT /B 0


:show_help
echo Usage: %~nx0 -p/--pre-flight -s/--version-sync
goto :eof


:get_supported_commit_list
set repo_url=%1
set initial_support_commit=%2
set latest_commit=%3
echo %initial_support_commit% %latest_commit%

for /f "tokens=*" %%i in ('git rev-list --topo-order %initial_support_commit%^..%latest_commit%') do echo %%i
goto :eof

:get_latest_commit_id
set repo_url=%1
for /f "tokens=1" %%i in ('git ls-remote "%repo_url%" HEAD ^| findstr /b "[0-9a-f]"') do set latest_commit_id=%%i
echo latest commit: %latest_commit_id%
goto :eof

:pre_flight_check
cd ../..
echo Start pre-flight check for WebUI...
call :get_latest_commit_id "https://github.com/AUTOMATIC1111/stable-diffusion-webui.git"
set LATEST_ROOT_COMMIT=%latest_commit_id%
echo Supported commits for WebUI:
call :get_supported_commit_list "https://github.com/AUTOMATIC1111/stable-diffusion-webui.git" "%INITIAL_SUPPORT_COMMIT_ROOT%" "%LATEST_ROOT_COMMIT%"
@REM set SUPPORTED_ROOT_COMMITS=%supported_commit_list%
for /f "tokens=*" %%i in ('git rev-parse HEAD') do set CUR_ROOT_COMMIT=%%i
echo Current commit id for WebUI: %CUR_ROOT_COMMIT%

echo Pre-flight checks complete.

goto :eof

:version_sync
echo Start version sync for WebUI, make sure the extension folder is empty...
cd ../../
set extension_folder=%1
if not exist %extension_folder%\ (
    echo The extension folder does not exist: %extension_folder%
    echo Please create it and run the script again.
    goto :eof
)

echo Syncing WebUI...

for %%n in (0, 1) do (
    set repo_folder=!REPO_FOLDER_LIST[%%n]!
    set repo_url=!REPO_URL_LIST[%%n]!
    echo repo_url !repo_url!
    call :get_latest_commit_id !repo_url!
    set latest_commit=!latest_commit_id!

    if not exist "%extension_folder%\!repo_folder!" (
        echo Cloning !repo_url! into !repo_folder!...
        git clone !repo_url! "%extension_folder%\!repo_folder!"
        cd "%extension_folder%\!repo_folder!"
        git checkout !latest_commit!
        cd %cd%
    ) else (
        echo Updating !repo_folder! to the latest commit...
        cd "%extension_folder%\!repo_folder!"
        git fetch origin
        git checkout !latest_commit!
        cd %cd%
    )
)

echo Version sync complete.

goto :eof

:parse_options
set options=%*
if not "%options%" == "" (
    for %%o in (%options%) do (
        if "%%o" == "-p" (
            call :pre_flight_check
            exit /b
        ) else if "%%o" == "--pre-flight" (
            call :pre_flight_check
            exit /b
        ) else if "%%o" == "-s" (
            call :version_sync "extensions"
            exit /b
        ) else if "%%o" == "--version-sync" (
            call :version_sync "extensions"
            exit /b
        ) else if "%%o" == "-h" (
            call :show_help
            exit /b
        ) else if "%%o" == "--help" (
            call :show_help
            exit /b
        ) else (
            echo Unknown option: %%o
        )
    )
) else (
    call :show_help
)
goto :eof