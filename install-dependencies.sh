# Travis had already installed Node.js with npm.
npm install bower -g
cd luiti/webui; bower install; cd -;

# Install eggs dependencies.
python setup.py install
