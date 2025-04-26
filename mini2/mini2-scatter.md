# Scattering Data Using Shared Memory

Mini 2 takes us from internal process parallelization to multi-process 
coordination with coordination between processes. This will be our first 
introduction to communication techniques that will give us a foundation 
to build the class project and mini 3. 

Two key concepts you will be working with:

   1. Shared Memory. An inter-process communication method to share 
      data between processes. 
   2. gRPC. A communication package for representing data, defining 
      remote procedure-based communication, and libraries. 

**The challenge for mini 2 is to explore overlays and research how 
coordination caching can be used. The description might seem 
a reach, an iterative approach will help ensue a clean base before 
adding additional functionality.**


## Mini Goals

*Hint. Do not repeat the mistakes of API ambassadors, slick presentations, 
design-by-buzzwords, or a blog post.*

There are two aspects of the mini that are critical to your team's success. 
The first (overlays) is where the bulk of the work resides, and is dependent 
upon the second (cache coherency). Let's go over the two and define what are 
successful metrics.


### Basecamp

So, the first challenge is to setup a working code base to start code 
explorations. The working baseline provides the ground truth to begin exploring
deeper concepts. This is an important step as it will determine your success or
fall. Among many skills that need to be mastered to complete the basecamp is a
base knowledge of C++ and Python.

Basecamp is a collection of processes (A,B,C,D,E) communicating with gRPC 
across two or more computers (1,2,3). In an overlay configuration like
AB, BC, BD, CE, and DE, where {A,B} are on 1 and {C,D,E} are on 2.

Preparation of your basecamp is critical. So, let's briefly go
over what it should and should not be.


#### Features

   * You will need servers (sinks) written in C++, and clients (sources) in Python
        * At least three.
   * Servers are asynchronous and trusted.
        * A limited number of switches will be provided. Coordination
          across teams is required, or obtaining one for your use.
   * Test/Run with at least two computers, three is better.
   * Organize your code into logical directories/trees
   * Use Cmake to build your C++ codes 
   * Use an IDE to write and debug your code. For instance, Eclipse has 
     a C++ and Python implementations, VisualStudio, and such.
   * Run from a shell (e.g., bash), test from a shell.
   * Testing harnesses for unit testing and remote (client) tests. Do
     you need one?


#### Avoidance hints

   * Do not use the IDE to run your code.
   * Remove or isolate dead or incomplete code from your main work
   * This is more work than mini 1 so, don't wait until the last week 
     to get started.
   * Details matter so, don't focus on making an application.  
   * Code organization is important

*Hint. gRPC source code has many examples to help design/code.*


### Discovery

It is not intended for this mini to have processes discover each other
dynamically. Rather a mapping can exist that provides guidance to each
process' edges (connections). This however, should not be hard coded 
within the code.


## Technical Constraints

Let us recap the guardrails mentioned above:

   * Python, C/C++, NOT javascript (node), java, C#, or other stuff.
   * Minimize third party libraries. Please do not use Anaconda stuff, 
     we are focusing at the service's communication transport layers.
   * No UX/UI stuff either. This includes test-based CLI menu systems.
   * No IDE VMs


### Using Basecamp

Once your basecamp has been established (how did you verify this?), we
can proceed onto the second part, caching vs replying (messaging). 
Let's ease the coding distractions; use the data, queries, and code 
from mini 1. 

First consider data can be received only by A. Consider A as a
portal (public facing) into a private network if that helps. You
have the option to implement the portal as python or keep it as
C++.

Of the communication styles, we are interested in one-way (request);
two-way (request-response) or one-to-many (broadcast) are not required.

The challenge - How do you use a one-way push to move data evenly
across the network.  For example, three or more clients (sources)
stream data to node A. Node A pushes data through the networked
peers (B,C,D, and E) looking for a home where the data should be
stored. Shared memory provides a means for the peers to indirectly
communicate amongst themselves. Consider the generative model from
our lecture slides.

It is expected the peers are independent (do not have a shared/common 
storage). This includes monitering or passing data. Each peer shouild 
accummulate a unique subset of data (no sharing, no replication, no 
mirroring). 

Lastly, only node A is allowed to recieve data from the gRPC sources 
(clients). The client must be ran in parallel.


## Using what we understand (mini 1)

Code experiences from mini-1 like for-loop parallelization scheduling, 
overloading, and testing practices can be insightful. 

To consider:

   * What do you consider are key performance/adoption indicators? 
       * The criteria to measure? Performance, time and effort, LoC, 
         cross-language, cross-OS, community support and tools, adoption 
         trajectories, hype curve, and so many others to choose from.  
       * Hint: Use more than one.
       * How do you measure time (dt)?
   * Realistic expectations and modeling - choose carefully. 
       * Professional organization and use of accepted style.
       * Using realistic data structures. I.e. strings for everything is not 
         a good choice. 
       * Coding a fixed response, chat-like, or a spoke-hub is a poor choice.


## Starting points

A couple code examples (shared memory, loop-grpc) will be (have been) uploaded to
labs area; they might provide an inspiration seed for your team to brainstorm what 
you want to consider. As well as any downloaded source code, like gRPC from gitHub.
Obviously, there are many approaches, do not limit yourselves to what is talked about
and in Canvas.

Second we touched on this in mini 1, the use of realistic data structures. I remind
everyone to be mindful in constructing code and test cases that represent professional
coding practices and expectations the code is a component of a larger project.


## Overlays

As mentioned above the network to construct is defined as a tree of processes. We are
not interested in a flat (one-to-many) design. 

This will create a couple challenges to you that a flat ignores. First, how does one
configure servers to know which they share edges with. Second, how do you forward
requests, and lastly, how do you provide context (match) requests to replies.

Edges are asynchronous but, do not have to be bi-directional? It is up to
your team to determine how to approach this.
