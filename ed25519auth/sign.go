package ed25519auth

// #include "sign.h"
import "C"

func Sign(challenge [32]byte, password []byte) (response [64]byte) {
	C.sign(
		(*C.uchar)(&response[0]),
		(*C.uchar)(&challenge[0]),
		(*C.uchar)(&password[0]),
		C.size_t(len(password)),
	)
	return
}
