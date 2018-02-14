pipeline {
    agent none

    environment {
        GIT_SOURCE_URL = 'https://github.com/BlueBrain/nexus-sourcing'
    }

    stages {
        stage("Review") {
            parallel {
                stage("StaticAnalysis") {
                    steps {
                        node("slave-sbt") {
                            sh 'export'
                            sh 'ls -las'
                            git url: "https://github.com/${env.ghprbGhRepository}", branch: "${env.sha1}"
                            sh 'sbt scalafmtSbtCheck scapegoat'
                        }
                    }
                }
                stage("Tests/Coverage") {
                    steps {
                        node("slave-sbt") {
                            sh 'export'
                            sh 'ls -las'
                            git url: "https://github.com/${env.ghprbGhRepository}", branch: "${env.sha1}"
                            sh 'sbt clean coverage coverageReport coverageAggregate'
                        }
                    }
                }
            }
        }
    }
}
