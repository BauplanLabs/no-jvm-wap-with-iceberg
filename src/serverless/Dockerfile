FROM public.ecr.aws/lambda/python:3.10

# install the few Python dependencies
RUN pip3 install --upgrade pip \
    && yum install gcc gcc-c++ -y \
    && pip3 install friendlywords==1.1.3 pyarrow==15.0.0 pyiceberg==0.6.0 pynessie==0.67.0 slack-sdk==3.27.1 --target "${LAMBDA_TASK_ROOT}"

ENV HOME=/home/aws

COPY app.py ${LAMBDA_TASK_ROOT}
COPY pyiceberg_patch_nessie.py ${LAMBDA_TASK_ROOT}
COPY monkey_patch.py ${LAMBDA_TASK_ROOT}

CMD [ "app.lambda_handler" ]
