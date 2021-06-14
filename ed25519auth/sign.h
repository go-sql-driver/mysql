#ifndef SIGN_H
#define SIGN_H

#include <stddef.h>

void sign(
  unsigned char sm[64],
  const unsigned char m[32],
  const unsigned char *pw, size_t pwlen
);

#endif
