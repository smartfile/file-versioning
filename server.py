import os
import time

from flask import Flask, redirect, render_template, request, Response, url_for
from werkzeug.utils import secure_filename

from backup import VersioningFS
from tests import Paths


app = Flask(__name__)

BASEDIR = os.path.abspath(os.path.dirname(__file__))
WORKING_DIR = os.path.join(BASEDIR, 'tmp')

paths = Paths(WORKING_DIR)

v = VersioningFS(paths.USER_FILES, paths.BACKUPS, paths.SNAPSHOT_INFO,
                 paths.TEMP)


class SingleFile(object):
    def __init__(self, name, path, version):
        self.name = name
        self.path = path
        self.version = version

    @property
    def size(self):
        return os.path.getsize(self.path)

    @property
    def modified(self):
        return time.ctime(os.path.getmtime(self.path))


@app.route('/')
def home():
    files = []
    for root, dirs, filenames in os.walk(paths.USER_FILES):
        for name in filenames:
            abs_path = os.path.join(root, name)
            path = abs_path.replace(paths.USER_FILES, '')
            display_path = path.replace("/", "")
            version = v.version(path)
            f = SingleFile(display_path, abs_path, version)
            files.append(f)

    return render_template("home.html", files=files)


@app.route('/file/<filename>')
def single_file(filename):
    version_number = v.version(filename)
    versions = [x for x in range(version_number + 1)]
    versions.reverse()
    return render_template("single-file.html", filename=filename,
                           versions=versions)


@app.route('/file/<filename>/<version>')
def version(filename, version):
    def generate(filename, version):
        version = int(version)
        f = v.open(filename, 'rb', version)
        for line in f:
            yield line

    headers={"Content-Disposition": "attachment;filename=%s" % (filename)}
    return Response(generate(filename, version), headers=headers)


@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        uploaded_file = request.files['file']
        filename = secure_filename(uploaded_file.filename).replace('/', '')
        f = v.open(filename, 'wb')
        f.write(uploaded_file.read())
        f.close()
    return redirect(url_for("home"))


if __name__ == '__main__':
    # create a directory where we can work
    for folder in paths.itervalues():
        if not os.path.exists(folder):
            os.makedirs(folder)

    app.run(debug=True, host='0.0.0.0')
