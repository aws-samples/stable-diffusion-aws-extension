在部署解决方案之前，建议您先查看本指南中有关架构图和区域支持等信息。然后按照下面的说明配置解决方案并将其部署到您的帐户中。

部署时间：约 25 分钟

## 前提条件
<!-- 用户需提前部署好本地的[Stable Diffusion WebUI](https://github.com/AUTOMATIC1111/stable-diffusion-webui)。 -->
用户需要提前准备一台运行linux系统的电脑

## 部署概述
在亚马逊云科技上部署本解决方案主要包括以下过程：

- 步骤1：在您的亚马逊云科技账户中启动Amazon CloudFormation模板。
- 步骤2：通过安装脚本安装插件Stable Diffusion AWS Extension。
- 步骤3: 配置API Url和API Token

<!-- - 步骤2：在您的现有Stable Diffusion WebUI上安装插件Stable Diffusion AWS Extension。 -->


## 部署步骤



### 步骤1：在您的亚马逊云科技账户中启动Amazon CloudFormation模板。

此自动化Amazon CloudFormation模板在亚马逊云科技中部署解决方案。

1. 登录到AWS管理控制台，点击链接[Stable-Diffusion-AWS-Extension.template](https://console.aws.amazon.com/cloudformation/home?#/stacks/create/template?stackName=stable-diffusion-aws&templateURL=https://aws-gcr-solutions.s3.amazonaws.com/stable-diffusion-aws-extension-github-mainline/latest/custom-domain/Stable-diffusion-aws-extension-middleware-stack.template.json){:target="_blank"}。
2. 默认情况下，该模版将在您登录控制台后默认的区域启动。若需在指定的Amazon Web Service区域中启动该解决方案，请在控制台导航栏中的区域下拉列表中选择。
3. 在**创建堆栈**页面上，确认Amazon S3 URL文本框中显示正确的模板URL，然后选择**下一步**。
4. 在**制定堆栈详细信息**页面，为您的解决方案堆栈分配一个账户内唯一且符合命名要求的名称。在**参数**部分，在**email**处输入一个正确的电子邮件地址，以便接收将来的通知。在**sdextensionapikey**字段中请输入一个包含数字和字母组合的20个字符的字符串；如果未提供，默认为"09876543210987654321"。在**utilscpuinsttype**选择Amazon EC2的实例类型，主要用于包括模型创建、模型合并等操作。点击**下一步**。
5. 在**配置堆栈选项**页面，选择**下一步**。
6. 在**审核**页面，查看并确认设置。确保选中确认模板将创建Amazon Identity and Access Management（IAM）资源的复选框。并确保选中AWS CloudFormation需要的其它功能的复选框。选择**提交**以部署堆栈。
7. 您可以在 AWS CloudFormation 控制台的 **状态** 列中查看堆栈的状态。您应该会在大约 15 分钟内收到**CREATE_COMPLETE**状态。

!!! Important "提示" 
    请及时检查您预留邮箱的收件箱，并在主题为“AWS Notification - Subscription Confirmation”的邮件中，点击“Confirm subscription”超链接，按提示完成订阅。

### 步骤2：通过安装脚本安装插件Stable Diffusion AWS Extension。
1. 在提前准备的运行linux的电脑的工作目录下，运行以下命令下载最新的安装脚本
```
wget https://raw.githubusercontent.com/awslabs/stable-diffusion-aws-extension/main/install.sh
```
2. 运行安装脚本
```
sh install.sh
```
3. 移步到install.sh下载的stable-diffusion-webui文件夹
```
cd stable-diffusion-webui
```
4. 对于不带GPU的机器，可以通过以下命令启动webui
```
./webui.sh --skip-torch-cuda-test
```
5. 对于带GPU的机器，可以通过以下命令启动webui
```
./webui.sh
```
### 步骤3: 配置API Url和API Token

1. 访问[AWS CloudFormation控制台](https://console.aws.amazon.com/cloudformation/){:target="_blank"}。

2. 从堆栈列表中选择方案的根堆栈，而不是嵌套堆栈。列表中嵌套堆栈的名称旁边会显示嵌套（NESTED）。

3. 打开输出（Outputs）标签页，找到**APIGatewayUrl**和**ApiGatewayUrlToken**对应的数值，并复制。

4. 打开Stable Diffusion WebUI中的**Amazon SageMaker**标签页，在**API URL**文本框粘贴步骤3得到的URL。在**API Token**输入步骤3得到的token。点击**Test Connection**，会得到**Successfully Connected**的确认信息。

5. 点击**Update Setting**更新配置文件，这样下次就能得到对应的信息
<!-- 1. 打开已部署的Stable Diffusion WebUI界面，进入**Extensions**标签页 - **Install from URL**子标签页，在**URL from extension's git repository**文本框输入本解决方案repository地址 [https://github.com/awslabs/stable-diffusion-aws-extension.git](https://github.com/awslabs/stable-diffusion-aws-extension.git)，点击**Install**。
2. 点击**Installed**子标签页，点击**Apply and restart UI**，WebUI会多出一个**Amazon SageMaker**标签页，表明已完成插件安装。 -->


<!-- ## 后续操作
堆栈创建成功后，您可以在AWS CloudFormation的输出（Outputs）标签页中查询相关信息。 -->
