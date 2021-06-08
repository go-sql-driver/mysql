#include <string.h>
#include "sign.h"
#include "sha512.h"
#include "ge.h"
#include "sc.h"

void sign(
  unsigned char sm[64],
  const unsigned char m[32],
  const unsigned char *pw, size_t pwlen
)
{
  unsigned char az[64];
  unsigned char nonce[64];
  unsigned char hram[64];
  unsigned char buff[96];
  ge_p3 A, R;

  sha512(pw,pwlen,az);
  az[0] &= 248;
  az[31] &= 63;
  az[31] |= 64;

  memmove(buff + 64,m,32);
  memmove(buff + 32,az + 32,32);
  sha512(buff + 32,64,nonce);

  ge_scalarmult_base(&A,az);
  ge_p3_tobytes(buff + 32,&A);

  sc_reduce(nonce);
  ge_scalarmult_base(&R,nonce);
  ge_p3_tobytes(buff,&R);
  memcpy(sm, buff, 32);

  sha512(buff,96,hram);
  sc_reduce(hram);
  sc_muladd(sm + 32,hram,az,nonce);
}
