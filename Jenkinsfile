pipeline {
    agent any

    stages {
        stage('Test') { ... }           // Phase 4 — already exists

        stage('Deploy ETL') { ... }     // Phase 4 — already exists

        stage('Run dbt') { ... }        // Phase 4 — already exists

        stage('Run ML Forecast') {      // ADD THIS
            steps {
                withCredentials([
                    string(credentialsId: 'SNOWFLAKE_ACCOUNT',  variable: 'SNOWFLAKE_ACCOUNT'),
                    string(credentialsId: 'SNOWFLAKE_USER',     variable: 'SNOWFLAKE_USER'),
                    string(credentialsId: 'SNOWFLAKE_PASSWORD', variable: 'SNOWFLAKE_PASSWORD')
                ]) {
                    sh 'pip install snowflake-connector-python[pandas] scikit-learn --quiet'
                    sh 'python ml_forecast.py'
                    sh 'dbt run --select ml_predictions --profiles-dir $DBT_PROFILES_DIR'
                }
            }
            post {
                always {
                    archiveArtifacts artifacts: 'model.pkl, metrics.json', fingerprint: true
                }
            }
        }

    }  // end stages
}      // end pipeline