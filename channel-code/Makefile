default:test

clean:
	mvn clean
	mvn eclipse:clean	
	rm -f channelcode.tar.gz

pkg:
	mvn clean compile assembly:single
	rm -f channelcode.tar.gz
	tar cvzf channelcode.tar.gz channel-topology target/ChannelCode-1.0-SNAPSHOT-jar-with-dependencies.jar 

ec:
	mvn eclipse:clean
	mvn eclipse:eclipse
