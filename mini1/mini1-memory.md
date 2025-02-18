# Parallelization

version 0.2a 

Focus: Threading and memory relationships in optimization.

Welcome to our first research project, mini1 (1 of 3), with a focus
on memory utilization within processes.

Mini projects are constructed to focus on one to two specific areas of
performance-based multi-process computing. Each mini is designed to 
provide research topics  along with developing skills in analysis, 
languages (C/C++ and Python), and analyical testing and verification. 
Opportunities exist to practice reverse engineering, open source code 
evaluation, strong/weak scaling, and performance metric gathering.


## Responsibilities

Minis are organized as a unstructured learning experience. Meaning
there are no set results of specific matched numerical values. 
Consequently, the are more degrees of freedom for you to explore 
and shape your investigations. 

**The burden of proof is yours to provide.** Please be sure to 
take this responsibility seriously. Here are some hints to help
set you on a course of success. 

   * Minis are not applications, features are not required
   * Depth of research and investigation is very important
   * Organization is expected to be professional 
   * Reports are paragraph formatted, and use supported data
     to support conclusions.
   * Do not run/test from IDE's VMs
   * Validation and verification using good practices
   * Realistic use of classes, abstraction, containers, and 
     primative data types


## Resources

Many resources are available for your use. Use of 
There are several resources already uploaded in Canvas to support this 
investigation. They are:

   * Data set. A data set is provided for testing your design.

     Source: NYC Data: Motor Vehicle Collisions 
     Size: Approx. 2.15M Rows (CSV)
     Ref: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data

   * Code. The course uses non-graded lab codes to provide examples
     of features and coding scenerios. You can use the **hello lab's** 
     code to provide a baseline for project organiation, compiling 
     (Cmake), and threading (for-loop, C++/OMP).
 
Other sources at your disposal include:

   * Open source code repositories
   * Language and package documentations
   * Research publications and papers
   * Online communities (e.g., stack overflow, xxxx)


## Objectives

There are three phases to the assignment. Each step (phase) is critical
to the success of your project.

### Phase 1

   1. Design and code with C/C++ a process to consume the provided 
      data set. Play particular attention to representing the data's
      fields as their primative types (e.g,, integers, floats).

      Your design must be an Object Oriented Design (Classes), placing 
      emphasise on abstraction, and as a library for long-term use.

      Suggestions, objects, virtual classes, templates, and coding 
      patterns (e.g., facade). Your code should provide features and 
      data through a set of APIs for data reading and basic range 
      searching.
 
      **Do not use threads for this part we are serial processing.**
 
      *Hint: Look at parser.tar.gz for tool suggestions*
      *Hint: What types of searches do you expect?*

   2. Benchmark your code against the data sets. Note think about
      what are reasonable areas where peformance would be a concern.

      Be sure to collect multiple executions (10+) to create an 
      average of your codes' performance. Document both successes and 
      failures in your coding attempts; results are for your report.
 
      **This becomes your baseline for further investigations.**

### Phase 2

   1. Apply parallelization (OpenMP, threading) to your code. Record 
      your results and compare them to your earlier benchmark.

      *Hint: This is where your code harness helps.*

### Phase 3

   1. Optimize your code by rewriting your data classes into a 
      vectorized organization (Object-of-Arrays) as well as changing
      any areas of the code you think could be improved. 

      Benchmark your results. Be sure to test how each modification 
      affects performance. 

      Remember in phase 3, performance is the highest goal. 

      *Hint: Do queries shape your design?*


### Language and Tools

The following is a list of languages and tools that are __required__ to
complete the project:

   Languages
   * C/C++ 
     * gcc/g++, v13 or newer
     * Clang (not apple's xcode) v16 or newer
   * Python3 can be used to generate graphs

   Libraries
   * None.

   Tools
   * Cmake (https://cmake.org)

These are suggestions that can help you with the mini:

   * lmod (https://github.com/TACC/Lmod)
   * Linters for C/C++ and Python
   * Editors (2+ recommended): neovim/vim, vscode, IDEs, etc.
   * Terminal: kitty, iterm2, OS native, etc
        ** You may want/need a font (https://www.nerdfonts.com) or two
        ** A shell scripting language (e.g., bash, tcsh,)
              *** https://www.freecodecamp.org/news/bash-scripting-tutorial-linux-shell-script-and-command-line-for-beginners/
              *** https://www.tutorialspoint.com/unix/shell_scripting.htm 
   * Clang analyzers, Valgrind suite, perf, or other like tools


### Restrictions and Limits

As this lab has been constructed with a specific set of learning and
discovery objectives, some aspects are artifically constrained. Yes,
many can be engineered out and exist in the wild yet, yet, the concepts 
remain valid. Therefore, there are several creativity boundaries that 
need to be put in place:

  * No third party libraries or services other than what has been 
    listed above. This means do not use databases. 
       * Parsing tools for reading CSV, and ploting/graphing are okay. 
  * Do not circumvent the spirit of the mini's goals/instructions.
  * AI likely is not going to help you code, and certainly will
    not help with an understanding of whys nevertheless, use it
    if you want. See below.


### What do you need to turn in?

There are three parts to return: 1) the code, 2) a report containing
your results, conclusions, and supporting facts and citations, and 
3) your presentation.

These three components should be contained in a archive file, `tar.gz`. 
E.g., teamducks.tar.gz.

One submission per team to canvas. Your team can (re)submit multiple 
times before the due date. **Please do not include test data.**


#### Regarding the presentation and Report

The presention is **exactly one slide** (imagine a poster) to be
presented to the class. The slide should be focusing on a single
important discovery your team made or a key experience you want
to pass to others. 

*Hint: It (slide) should be a key finding by your team. Something 
of significant finding you would like to share. Now, wht it should 
not be: A project summary, a diagram of your classes, a tutorial, 
and most definitely not vague.*

The report is the supporting facts and approach leading to conclusion
and presentation. Please include a discussion on how the team achieved 
a solution, findings/conclusions, citations, and individual contributions.

This is your team's chance to showcase what your research and coding
uncovered. Please include testing results (tabular and graph formats),
and observations backing up your conclusions and recommendations. Lastly, 
don't discard failed attempts, irregardless whether they helped these are
important data points.

*Hint: Supporting your conclusions largely affects your points.*


### What about AI?

Well, this has been the elephant in the room for many. So, how 
do we proceed. This will be interesting. If you choose to use 
AI to help, know this.

   * Your skills to use AI verses knowing how to research, and
     code is the tension that you must balance. Interviewers 
     are most interested in your knowledge and reasoning skills.
   * Demarked in code which is AI, failure to do nulls the 
     assignment
   * Reports also must be demarked and citations used.
