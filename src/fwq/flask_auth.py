from __future__ import annotations

import traceback
from typing import Optional

from flask import flash, render_template, redirect, url_for, request, current_app, session
from flask_login import LoginManager, login_user, logout_user
from werkzeug.security import generate_password_hash, check_password_hash

from .flask import FlaskApp


class User:

    def __init__(self, user_dict):
        self._user_info = user_dict
        if user_dict is not None:
            self._id = user_dict['id']
            self._name = f"{user_dict['name']}"
            self._email = user_dict['email']
            self._groups = user_dict['groups']
            self._active = user_dict['is_active']
            self._password_hash = user_dict['hashed_password']
            self._anonymous = False
        else:
            self._active = False
            self._anonymous = True

        self._authenticated = False

    def __str__(self):
        return f"User(id={self._id}, name={self._name}, email={self._email}, is_active={self._active})"

    @staticmethod
    def for_id(user_id) -> Optional[User]:
        return None

    @staticmethod
    def for_email(email) -> Optional[User]:
        return None

    def get_user_info(self):
        return self._user_info

    def is_member_of(self, required_groups):
        result = False
        if self._groups is not None:
            if required_groups is None:
                result = True
            else:
                for group in required_groups:
                    if group in self._groups:
                        result = True
                        break
        return result

    def set_active(self, active_flag):
        self._active = active_flag

    def is_active(self):
        return self._active

    def get_id(self):
        return str(self._id)

    def set_authenticated(self, authenticated_flag):
        self._authenticated = authenticated_flag

    def is_authenticated(self):
        return self._authenticated

    def set_anonymous(self, anonymous_flag):
        self._anonymous = anonymous_flag

    def is_anonymous(self):
        return self._anonymous

    def get_name(self):
        return self._name

    def get_password_hash(self):
        return self._password_hash

    def check_password(self, password):
        return check_password_hash(self.get_password_hash(), password)


class FlaskAuthApp(FlaskApp):
    def __init__(self, config):
        super().__init__(config)

        self._app_user_group = None
        if "app_user_group" in config:
            self._app_user_group = config["app_user_group"]

    def init(self):
        # --------------------------------------------------------------------------- #
        # Initialize the auth manager
        login_manager = LoginManager()
        login_manager.login_view = 'auth.login'
        login_manager.init_app(self._flask_app)

        @login_manager.user_loader
        def load_user(user_id):
            user = User.for_id(user_id)
            print(f"user: {user}")
            if user is not None:
                if user.is_active():
                    print(f"user.is_active(): {user.is_active()}")
                    print(f"app_user_group: {self._app_user_group}")
                    if self._app_user_group is not None:
                        print(f"app_user_group: {self._app_user_group}")
                        print(f"user is member: {user.is_member_of(self._app_user_group)}")
                        if user.is_member_of(self._app_user_group):
                            session['user'] = user.get_user_info()
                            # g.user = user
                            return user
                    else:
                        session['user'] = user.get_user_info()
                        # g.user = user
                        return user
            # g.pop("user", None)
            session.pop("user", None)
            flash("Authorization failed!", 'error')
            return None

    def routes(self):

        app = self._flask_app
        @app.route('/auth/login')
        def login():
            # if "user" not in g:
            if "user" not in session:
                return render_template('login.html')
            else:
                logout_user()
                return redirect(url_for("hello.hello"))

        @app.route('/auth/login', methods=['POST'])
        def login_post():
            try:
                email = request.form.get('email')
                password = request.form.get('password')

                app_name = current_app.config['app_name']
                user = User.for_email(email)
                if user is None or not user.is_active():
                    raise Exception(f"Login failed. Please check your credentials.")
                else:
                    if user.check_password(password):
                        if login_user(user):
                            print(f"AUTHENTICATED: {user}")
                            flash(f"Welcome {user.get_name()}!")
                    else:
                        raise Exception(f"Login failed. Please check your credentials.")

                return redirect(url_for('hello.hello'))

            except Exception as e:
                traceback.print_exc()
                msg = f'ERROR: {e}'
                flash(msg, 'error')
                print(msg)
                return redirect(url_for('auth.login'))

        @app.route('/auth/logout')
        def logout():
            try:
                logout_user()

            except Exception as e:
                traceback.print_exc()
                msg = f'ERROR: {e}'
                flash(msg, 'error')
                print(msg)

            return redirect(url_for('hello.hello'))

