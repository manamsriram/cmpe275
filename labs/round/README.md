## rounding lab

By now you know about how openMP creates an abstraction of the parallel region 
and platform. These parallel regions (e.g., for-loop) are an interesting study
in how nuanced parallalizing code can be. 

So let's dig into this simple example of loop (array processing in this case)
execution. 
 

### compiling 

Like the hello lab, round uses CMake to build the executable. Look at the build,
CMakelists.txt, configuration file and you will see additional configuration settings

   - executable directory
   - platform specific code (needs to be detecting, TBD)
   - an additional dependency, boost.


### running

Right, run the executable...questions:

   - What is happening?
   - How do you make the code more consistent?
