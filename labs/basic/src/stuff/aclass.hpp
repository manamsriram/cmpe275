#ifndef ACLASS_HPP
#define ACLASS_HPP

namespace amodule {

/**
 * @brief class setup
 */
class AClass {
   public: 
     AClass() : x(0) {}
     virtual ~AClass() {}

     void fn(int h);

  private:  
 
     int x;
};

} // amod

#endif
