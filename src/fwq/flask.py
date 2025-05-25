from gwerks import emitter, environment

from gwerks.docker import DockerBase

import os
import signal
import datetime

import pytz
from setproctitle import setproctitle
from waitress.server import create_server
from flask import Flask, render_template, session


class FlaskApp:
    def __init__(self, config):

        # do not buffer output so we can see it as it happens
        # os.environ["PYTHONUNBUFFERED"] = "1"

        if "port" not in config:
            raise Exception("port is required")
        self._port = config["port"]

        if "app_base" not in config:
            raise Exception(f"The app base has not been set")
        self._app_base = config["app_base"]
        self._app_base = f"{os.path.abspath(self._app_base).replace("\\", " / ")}"

        if "process_app_name" not in config:
            raise Exception(f"The process app name has not been set")
        self._process_app_name = config["process_app_name"]

        # get the secret key
        if "secret_key" not in config:
            raise Exception(f"The secret key has not been set")
        self._secret_key = config["secret_key"]

        self._app_pkg = __name__.split('.')[0]
        if "app_pkg" in config:
            self._app_pkg = config["app_pkg"]

        self._addr = "0.0.0.0"
        if "listen_addr" in config:
            self._addr = config["listen_addr"]

        self._threads = 8
        if "threads" in config:
            self._threads = config["threads"]

        # --------------------------------------------------------------------------- #
        # Initialize Flask
        # https://www.palletsprojects.com/p/flask/

        print(f"app_base: {self._app_base}")
        app_dir = os.path.abspath(self._app_base)
        print(f"app_dir: {app_dir}")
        templates_dir = os.path.join(app_dir, "templates")
        print(f"templates_dir: {templates_dir}")
        static_dir = os.path.join(app_dir, "static")
        print(f"static_dir: {static_dir}")

        self._flask_app = Flask(self._app_pkg,  template_folder=templates_dir,  static_folder=static_dir)
        self._flask_app.jinja_env.add_extension('jinja2.ext.loopcontrols')
        self._flask_app.config.from_mapping(
            SECRET_KEY=self._secret_key
        )
        self._flask_app.config['app_name'] = self._process_app_name

        self._do_init()

        # go do all the things that need done once the server is up
        self.on_startup()

    def _do_init(self):
        self.init()
        self._base_context_processors()
        self._base_template_filters()
        self._base_routes()
        self.routes()

    # subclasses should override this to do their own initialization
    def init(self):
        pass

    # subclasses should override this to add routes
    def routes(self,):
        pass

    # subclasses should override this to add startup functionality
    def on_startup(self):
        print(f"*{self._process_app_name}* on_startup")

    # subclasses should override this to add routes
    def on_shutdown(self):
        print(f"*{self._process_app_name}* on_shutdown")

    def _base_routes(self):
        app = self._flask_app

        @app.route("/get_flashes")
        def get_flashes():
            return render_template("_flashes.html")

    def _base_context_processors(self):
        app = self._flask_app

        @app.context_processor
        def inject_current_user():
            cur_user = None
            if "user" in session:
                cur_user = session['user']
            return dict(current_user=cur_user)

        @app.context_processor
        def inject_version():
            this_dir = os.path.dirname(os.path.realpath(__file__))
            v_file = os.path.join(this_dir, "version.txt")
            if os.path.exists(v_file):
                with open(v_file, "r") as f:
                    version = f.read()
            else:
                version = "SANDBOX"

            return dict(version=version)

        @app.context_processor
        def inject_runtime_environment():
            rt_env = environment()
            return dict(rt_env=rt_env)

    def _base_template_filters(self):
        app = self._flask_app

        @app.template_filter("date_and_time")
        def timezone_and_format_filter(value, to_tz='US/Eastern', datetime_format="%Y-%m-%d %H:%M:%S"):
            target_tz = pytz.timezone(to_tz)
            if value and isinstance(value, datetime.datetime):
                target_datetime = value.astimezone(target_tz)
                return target_datetime.strftime(datetime_format)
            return ""

        @app.template_filter()
        def timezone_and_date_filter(value, to_tz='US/Eastern', datetime_format="%Y-%m-%d"):
            target_tz = pytz.timezone(to_tz)
            if value and isinstance(value, datetime.datetime):
                target_datetime = value.astimezone(target_tz)
                return target_datetime.strftime(datetime_format)
            return ""

        @app.template_filter()
        def command_line_display_filter(value):
            if value and value.get("--") >= 0:
                results = "<b>"
                results += value[:value.get("--")]
                results += "</b>"
                results += value[value.get("--"):]
            else:
                results = value
            return results

    # --------------------------------------------------------------------------- #
    # Runs a flask app with waitress from the command line
    def run(self):

        setproctitle(self._process_app_name)

        # handle process termination signals
        def signal_handler(signum, frame):
            signame = signal.Signals(signum).name
            print(f"Signal [{signame}] received, shutting down...")
            self.on_shutdown()
            waitress_server.close()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGHUP):
            signal.signal(sig, signal_handler)

        print(f"waitress -addr: {self._addr}, port: {self._port}, threads: {self._threads}")
        waitress_server = create_server(self._flask_app, host=self._addr, port=self._port, threads=self._threads, asyncore_use_poll=False)
        print(f"{self._process_app_name} Ready!")
        waitress_server.run()


# --------------------------------------------------------------------------- #
# Runs the surveys_app_project from the command line
# def main():
#     app_base = pathlib.Path(__file__).parent.resolve()
#     flask_app = create_app(app_base=app_base)
#     run_with_waitress(flask_app)


# --------------------------------------------------------------------------- #
# Runs the surveys_app_project from the command line
# if __name__ == "__main__":
#     main()



@emitter()
class FlaskContainer(DockerBase):
    def __init__(self, config):

        # container_name, network: Network, listen_addr=, port=, data_volume_host=None, max_message_size=
        config['image_name'] = "maateen/docker-beanstalkd"
        super().__init__(config)

        self._addr = "0.0.0.0"
        if "listen_addr" in config:
            self._addr = config["listen_addr"]

        self._port = "11300"
        if "port" in config:
            self._port = config["port"]

        self._data_volume = "/data"
        if "data_volume" in config:
            self._data_volume = config["data_volume"]

        self._data_volume_host = None
        if "data_volume_host" in config:
            self._data_volume_host = config["data_volume_host"]

        self._max_message_size = "65535"
        if "max_message_size" in config:
            self._max_message_size = config["max_message_size"]

        self._published_ports.append(self._port)
        if self._data_volume_host:
            self._volume_mappings.append([self._data_volume_host, self._data_volume])

    def start(self):
        self.get_docker_network().create()

        cmd = ""
        cmd += f"-V "
        cmd += f"-b {self._data_volume} "
        if self._addr:
            cmd += f"-l {self._addr} "
        if self._port:
            cmd += f"-p {self._port} "
        if self._max_message_size:
            cmd += f"-z {self._max_message_size} "

        self.docker_run(cmd)

    def stop(self):
        self.docker_stop()
