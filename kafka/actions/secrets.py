from . import action_store

def get_secrets():
    host = action_store.secrets.get("JENKINS_URL")
    username = action_store.secrets.get("JENKINS_USER")
    password = action_store.secrets.get("JENKINS_PASSWORD")
    return host, username, password