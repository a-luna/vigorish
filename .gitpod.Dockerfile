FROM gitpod/workspace-base:latest

RUN echo "CI version from base"

### NodeJS ###
USER gitpod
ENV NODE_VERSION=16.13.0
ENV TRIGGER_REBUILD=1
RUN curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | PROFILE=/dev/null bash \
  && bash -c ". .nvm/nvm.sh \
  && nvm install $NODE_VERSION \
  && nvm alias default $NODE_VERSION \
  && npm install -g typescript yarn node-gyp" \
  && echo ". ~/.nvm/nvm.sh"  >> /home/gitpod/.bashrc.d/50-node
ENV PATH=$PATH:/home/gitpod/.nvm/versions/node/v${NODE_VERSION}/bin

### Python ###
USER gitpod
RUN sudo install-packages python3-pip

ENV PATH=$HOME/.pyenv/bin:$HOME/.pyenv/shims:$PATH
RUN curl -fsSL https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash \
  && { echo; \
  echo 'eval "$(pyenv init -)"'; \
  echo 'eval "$(pyenv virtualenv-init -)"'; } >> /home/gitpod/.bashrc.d/60-python \
  && pyenv update \
  && pyenv install 3.10.1 \
  && pyenv global 3.10.1 \
  && python3 -m pip install --no-cache-dir --upgrade pip \
  && python3 -m pip install --no-cache-dir --upgrade \
  setuptools wheel virtualenv flake8 pydocstyle twine \
  && sudo rm -rf /tmp/*USER gitpod
ENV PYTHONUSERBASE=/workspace/.pip-modules \
  PIP_USER=yes
ENV PATH=$PYTHONUSERBASE/bin:$PATH

# SQLite

RUN sudo apt-get update && sudo apt-get install -y build-essential uuid-dev sqlite3

# Add aliases

RUN echo 'alias python=python3' >> ~/.bashrc && \
  echo 'alias pip=pip3' >> ~/.bashrc && \
  echo 'alias font_fix="python3 $GITPOD_REPO_ROOT/.vscode/font_fix.py"' >> ~/.bashrc

# Local environment variables

ENV PORT="8080"
ENV IP="0.0.0.0"

