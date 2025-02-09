#ifdef ACLASS_HPP
#define ACLASS_HPP

namespace amodule {

/**
 * @brief class setup
 */
class BClass {
   public: 
     AClass() : x(0) {}
     virtual ~AClass() {}

     void fn(int h) { x=h; };

  private:  
 
     int x;
};

} // amod

#endif
