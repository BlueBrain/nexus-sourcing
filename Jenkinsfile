pipeline {
  stages {
    stage("Review") {
      parallel {
        stage("StaticAnalysis") {
          node("slave-sbt") {
            echo 'Static Analysis'
          }
        }
        stage("TestsWithCoverage") {
          node("slave-sbt") {
            echo 'Tests with Coverage'
          }
        }
      }
    }
  }
}

