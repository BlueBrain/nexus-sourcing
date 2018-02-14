pipeline {
  agent none
  stages {
    stage("Review") {
      parallel {
        stage("StaticAnalysis") {
          steps {
            node("slave-sbt") {
              echo 'Static Analysis'
            }
          }
        }
        stage("TestsWithCoverage") {
          steps {
            node("slave-sbt") {
              echo 'Tests with Coverage'
            }
          }
        }
      }
    }
  }
}

