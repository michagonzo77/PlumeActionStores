from kubiya import ActionStore


action_store = ActionStore("kafka", "0.1.0")
action_store.uses_secrets(["JENKINS_URL", "JENKINS_PASSWORD", "JENKINS_USER"])