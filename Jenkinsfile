String version = env.BRANCH_NAME
Boolean isRelease = version ==~ /v\d+\.\d+\.\d+.*/
Boolean isPR = env.CHANGE_ID != null

pipeline {
    agent none
    tools {
        jdk 'jdk11'
    }
    stages {
        stage("Review") {
            when {
                expression { isPR }
            }
            parallel {
                stage("StaticAnalysis") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh 'sbt clean scalafmtCheck test:scalafmtCheck scalafmtSbtCheck scapegoat'
                        }
                    }
                }
                stage("Tests/Coverage") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh "sbt clean coverage test coverageReport coverageAggregate"
                            sh "curl -s https://codecov.io/bash >> ./coverage.sh"
                            sh "bash ./coverage.sh -t `oc get secrets codecov-secret --template='{{.data.nexus_sourcing}}' | base64 -d`"
                        }
                    }
                }
            }
        }
        stage("Release") {
            when {
                expression { isRelease }
            }
            steps {
                node("slave-sbt") {
                    checkout scm
                    sh 'sbt releaseEarly'
                }
            }
        }
        stage("Report Coverage") {
            when {
                expression { !isPR }
            }
            steps {
                node("slave-sbt") {
                    checkout scm
                    sh "sbt clean coverage test coverageReport coverageAggregate"
                    sh "curl -s https://codecov.io/bash >> ./coverage.sh"
                    sh "bash ./coverage.sh -t `oc get secrets codecov-secret --template='{{.data.nexus_sourcing}}' | base64 -d`"
                }
            }
        }
    }
}
