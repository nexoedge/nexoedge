# Code Contribution Guidelines

## Compiler version

- gcc 11 or above
- g++ 11 or above, supporting c++17 standard

## Source Code File

### File Naming

- All files are named using 
- Files extensions
  - Header files in C: `.h`
  - Source files in C: `.c`
  - Header files in C++: `.hh`
  - Source files in C++: `.cc`

### File Format

- All header files should be enclosed with `#define` to avoid multiple inclusion. The format of the symbol name should be `__<PATH>_<FILE>__`, e.g., for hello/hello_world.hh,
  
```cpp
#ifndef __HELLO_HELLO_WORLD_HH__
#define __HELLO_HELLO_WORLD_HH__

...


#endif //define __HELLO_HELLO_WORLD_HH__
```

## Code Style Guide

Unless otherwise specified below, we follow the coding style in the guideline -- [Google coding style for C++][google_gpp_code_style].

### Indentation

* Use **4 spaces** for each level of indentation

### Variables and Constants

- For C++ code, use Camel case (e.g., `helloWorld`) to name variables
  - First character of variables must be in lowercase
  - For C code, use underscores to delimit words in the name of variables without using any capital letters (e.g., `hello_world`)
  - For both C++ and C code, use capital letters for the name of constants, and delimit with underscores whenever appropriate (e.g., `HELLO_WORLD`)

### Class
  - Private and protected variables should start with one underscore (e.g., `_helloWorld`)
  - Public variables should NOT start with underscores
  - For functions that are not used by any other classes, always put them in the private section.

### Namespace
  - Avoid `using namespace ...;` (whenever possible)
  - For long namespace, consider defining an alias, e.g., alias `google::cloud::storage` as `gcs`: `namespace gcs = google::cloud::storage;`
    
## Code Documentation

We follow the documentation style in [Doxygen for C and C++ codes][doxygen_c_cpp].
    
For every function, use the following comment style to document its usage, e.g., parameters and return values. Skip fields if they are not used. Make sure all conditions and assumptions of the parameters are described, and the meaning of all possible return values are listed.
    
For every class variable, use the following comment style to document its meaning/purpose.
    
```cpp
class Arithmetics {
public:
    /**
     * (Function Description) Obtain the sum of two variables 
     *
     * @param[in] a
     * @param[in] b
     *
     * @return sum of a and b
     *
     * @remarks 
     * @see difference()
     **/
    int sum(int a, int b);

private:
    int _opCnt; /**< operation count */
};
```

## Developer Certificate of Origin

By making a contribution to this project, I certify that:

1. The contribution was created in whole or in part by me and I have the right to submit it under the open source license indicated in the file; or

2. The contribution is based upon previous work that, to the best of my knowledge, is covered under an appropriate open source license and I have the right under that license to submit that work with modifications, whether created in whole or in part by me, under the same open source license (unless I am permitted to submit under a different license), as indicated in the file; or

3. The contribution was provided directly to me by some other person who certified 1., 2. or 3. and I have not modified it.

4. I understand and agree that this project and the contribution are public and that a record of the contribution (including all personal information I submit with it, including my sign-off) is maintained indefinitely and may be redistributed consistent with this project or the open source license(s) involved.



[doxygen_c_cpp]: http://www.doxygen.nl/manual/docblocks.html#cppblock

[google_gpp_code_style]: https://google.github.io/styleguide/cppguide.html
