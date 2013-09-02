/*
 * crypt323.go
 *
 * Implementation of the MySQL 323 encryption algorithm
 *
 * 27.08.2013, Klaus Hennemann
 */

package mysql

import (
	"math"
)

/*--------------------------------------------------------------------------*/
/*	MySQL < 4.1 pseudo random number generator			    */
/*--------------------------------------------------------------------------*/

const seed_max = 0x3FFFFFFF

type rand struct {
	seed1 uint32
	seed2 uint32
}

/*!
 * Initialize the random number generator
 */
func newRand(seed1, seed2 uint32) *rand {
	r := new(rand)
	r.seed1 = seed1 % seed_max
	r.seed2 = seed2 % seed_max

	return r
}

/*!
 * Generate one random float64 number.
 */
func (r *rand) Float64() float64 {
	r.seed1 = (3*r.seed1 + r.seed2) % seed_max
	r.seed2 = (r.seed1 + r.seed2 + 33) % seed_max

	return float64(r.seed1) / float64(seed_max)
}

/*--------------------------------------------------------------------------*/
/*	Message encryption functions					    */
/*--------------------------------------------------------------------------*/

func hash(buf []byte) (value [2]uint32) {
	var add uint32 = 7

	value[0] = 1345345333
	value[1] = 0x12345671

	for _, b := range buf {
		/* skip spaces and tabs in password */
		if b == ' ' || b == '\t' {
			continue
		}

		tmp := uint32(b)
		value[0] ^= (((value[0] & 63) + add) * tmp) + (value[0] << 8)
		value[1] += (value[1] << 8) ^ value[0]
		add += tmp
	}

	value[0] &= 0x7FFFFFFF
	value[1] &= 0x7FFFFFFF
	return
}

/*!
 * Scramble \a message with \a password. Only the first 8
 * bytes of \a message are scrambled.
 */
func Crypt323(message []byte, password []byte) []byte {
	if len(password) <= 0 {
		return nil
	}

	hash_msg := hash(message[:8])
	hash_pwd := hash(password)

	rand := newRand(hash_pwd[0]^hash_msg[0],
		hash_pwd[1]^hash_msg[1])

	var out [8]byte

	for n := range out {
		out[n] = byte(math.Floor(31*rand.Float64()) + 64)
	}

	mask := byte(math.Floor(31 * rand.Float64()))

	for n := range out {
		out[n] ^= mask
	}

	return out[:]
}
