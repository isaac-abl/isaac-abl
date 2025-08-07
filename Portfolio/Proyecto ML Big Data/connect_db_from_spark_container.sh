# The following command can be executed from inside the spark container to
# connect to the PostgreSQL DB, that can be loaded with db/run_image.sh
# This is a convenience in case access is needed from within the Spark
# container, mostly for troubleshooting. Remember the password assigned
# is testPassword
psql -h host.docker.internal -p 5433 -U postgres



spark-submit --driver-class-path postgresql-42.2.14.jar --jars postgresql-42.2.14.jar ml.py

spark-submit --driver-class-path postgresql-42.2.14.jar --jars postgresql-42.2.14.jar main1.py students_1.csv students_2.csv