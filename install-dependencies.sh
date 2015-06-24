# Travis had already installed Node.js with npm.
npm install bower -g
cd luiti/webui; bower install; cd -;

# Install eggs dependencies.

# Fix => Reading http://pyparsing.wikispaces.com/ error: timed out
pip install pyparsing --retries 10 --timeout 60
python setup.py install
