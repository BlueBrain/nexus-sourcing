pipeline {
  agent none
  stages {
    stage("Review") {
      parallel {
        stage("StaticAnalysis") {
          steps {
            node("slave-sbt") {
              sh 'sbt scalafmtSbtCheck scapegoat'
            }
          }
        }
        stage("Tests/Coverage") {
          steps {
            node("slave-sbt") {
              sh 'sbt clean coverage coverageReport coverageAggregate'
            }
          }
        }
      }
    }
  }
}
