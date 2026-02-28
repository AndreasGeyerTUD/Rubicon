#pragma once

#ifdef VERBOSE_OUTPUT
    #define VLOG(msg) std::cout << "[VERBOSE] " << msg << std::endl;
#else
    #define VLOG(msg) ((void)0)
#endif