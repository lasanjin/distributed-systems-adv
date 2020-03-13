## Reliable Ordered Multicaster: a reliable and ordered multicast protocol
We designed and built a module that provides reliable and ordered multicast. The module is driven by a provided GUI and uses pre-setup TCP connections ([mcgui package](http://www.cse.chalmers.se/edu/year/2014/course/TDA297/labs/mcgui-javadoc/index.html)). The module copes with crashing processes, but not rejoining processes.

Our protocol is based on a fixed sequencer algorithm, ring leader election algorithm, reliable multicast algorithm and causal-total ordering. Read more [here](report/report.pdf)

## How to run
1. Compile and execute
```
$ ./build && ./run
```
