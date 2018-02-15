pipeline {
    agent none

    stages {
        stage("Review") {
            parallel {
                stage("StaticAnalysis") {
                    steps {
                        node("slave-sbt") {
                            sh 'export'
                            checkout scm
                            sh 'sbt clean scalafmtSbtCheck scapegoat'
                        }
                    }
                }
                stage("Tests/Coverage") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh 'sbt clean coverage test coverageReport coverageAggregate'
                        }
                    }
                }
            }
        }
    }
}
