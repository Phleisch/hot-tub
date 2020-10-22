<div align="center">

# Hot Tub

![hot tub logo](/media/logo.png "Hot Tub Logo")

</div>



Project for Data-Intensive Computing (ID2221) at KTH Royal Institute of
Technology. Created by Kai Fleischman & Martin Schuck.

## Structure
![hot tub structure](/media/hot_tub.jpeg "Hot Tub internal structure")

## DockerCompose

All services can be started by first cleaning the environment:
docker-compose rm -svf

This avoids the ephemeral error with kafka. Then launch
docker-compose up
