nohup java -Xms128m -Xmx128m -server \
-cp \
bin/DORunner.jar:\
bin:\
\
lib/AdOpt.jar:\
lib/adwords-api-8.6.0.jar:\
lib/mysql-connector-java-5.1.13-bin.jar:\
lib/log4j-1.2.15.jar:\
lib/commons-cli-1.2.jar:\
lib/org_codehaus_jackson__jackson_mapper_asl-1.4.0_1.jar:\
lib/org_codehaus_jackson__jackson_core_asl-1.4.0_1.jar:\
lib/junit-4.9b2.jar:\
\
lib/sharethis.common.jar:\
lib/rtb.common.jar:\
lib/jetty-6.1.22.jar:\
lib/jetty-util-6.1.22.jar:\
lib/servlet-api-2.5-6.1.14.jar \
\
com.sharethis.doreport.server.ReportServer -iap bin/res/dors.properties -il4jp bin/res/log4j.properties -ipb /mnt/dors -opb . -dop bin/res/deliveryOptimizer.properties > /home/btdev/products/deliveryOptimizer/logs/ReportServer.log 2>&1 &
