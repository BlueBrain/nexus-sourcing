pipeline {
  agent none
  stages {
    stage("Review") {
      parallel {
        stage("StaticAnalysis") {
          steps {
            node("slave-sbt") {
              sh 'export'
              sh 'ls -las'
              git url:''
              sh 'sbt scalafmtSbtCheck scapegoat'
            }
          }
        }
        stage("Tests/Coverage") {
          steps {
            node("slave-sbt") {
              sh 'export'
              sh 'ls -las'
              sh 'sbt clean coverage coverageReport coverageAggregate'
            }
          }
        }
      }
    }
  }
}
