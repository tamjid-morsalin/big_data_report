from flask import Flask, send_from_directory
import os

app = Flask(__name__)

@app.route('/file/<string:filename>',methods=['GET'])
def download_image(filename):
    return send_from_directory(os.getcwd() + "/", path=filename, as_attachment=True)

if __name__ == '__main__':
    app.run(host= '127.0.0.1',port='6000',debug=True)