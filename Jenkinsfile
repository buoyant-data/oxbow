/*
 * This Jenkinsfile is for internal use
 */

pipeline {
  agent {
    label 'rust'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }
    stage('Prepare') {
      steps {
        sh './ci/setup.sh'
      }
    }
    stage('Build') {
      steps {
        sh './ci/build.sh'
      }
    }
    stage('Test') {
      steps {
        sh './ci/test.sh'
      }
    }
    stage('Release') {
      steps {
        sh './ci/build-release.sh'
        archiveArtifacts artifacts: 'target/lambda/**/*.zip', fingerprint: true, onlyIfSuccessful: true
      }
    }
  }
}


// vim: ft=groovy sw=2 ts=2 et