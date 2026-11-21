/*-------------------------------------------------------------------------
 *
 * cipher.c
 *	  Shared frontend/backend for cryptographic functions
 *
 * This is the set of in-core functions used when there are no other
 * alternative options like OpenSSL.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/cipher.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/cipher.h"

static void cipher_failure(void) pg_attribute_noreturn();


PgCipherCtx *
pg_cipher_ctx_create(int cipher pg_attribute_unused(),
					 unsigned char *key pg_attribute_unused(),
					 int klen pg_attribute_unused(),
					 bool enc pg_attribute_unused())
{
	cipher_failure();
	return NULL;				/* keep compiler quiet */
}

void
pg_cipher_ctx_free(PgCipherCtx *ctx pg_attribute_unused())
{
	cipher_failure();
}

int
pg_cipher_blocksize(PgCipherCtx *ctx pg_attribute_unused())
{
	cipher_failure();
	return -1;					/* keep compiler quiet */
}

bool
pg_cipher_encrypt(PgCipherCtx *ctx pg_attribute_unused(),
				  const int cipher pg_attribute_unused(),
				  const unsigned char *plaintext pg_attribute_unused(),
				  const int inlen pg_attribute_unused(),
				  unsigned char *ciphertext pg_attribute_unused(),
				  int *outlen pg_attribute_unused(),
				  const unsigned char *iv pg_attribute_unused(),
				  const int ivlen pg_attribute_unused(),
				  unsigned char *outtag pg_attribute_unused(),
				  const int taglen pg_attribute_unused())
{
	cipher_failure();
	return false;				/* keep compiler quiet */
}

bool
pg_cipher_decrypt(PgCipherCtx *ctx pg_attribute_unused(),
				  const int cipher pg_attribute_unused(),
				  const unsigned char *ciphertext pg_attribute_unused(),
				  const int inlen pg_attribute_unused(),
				  unsigned char *plaintext pg_attribute_unused(),
				  int *outlen pg_attribute_unused(),
				  const unsigned char *iv pg_attribute_unused(),
				  const int ivlen pg_attribute_unused(),
				  unsigned char *intag pg_attribute_unused(),
				  const int taglen pg_attribute_unused())
{
	cipher_failure();
	return false;				/* keep compiler quiet */
}

bool
pg_cipher_keywrap(PgCipherCtx *ctx pg_attribute_unused(),
				  const unsigned char *plaintext pg_attribute_unused(),
				  const int inlen pg_attribute_unused(),
				  unsigned char *ciphertext pg_attribute_unused(),
				  int *outlen pg_attribute_unused())
{
	cipher_failure();
	return false;				/* keep compiler quiet */
}

bool
pg_cipher_keyunwrap(PgCipherCtx *ctx pg_attribute_unused(),
					const unsigned char *ciphertext pg_attribute_unused(),
					const int inlen pg_attribute_unused(),
					unsigned char *plaintext pg_attribute_unused(),
					int *outlen pg_attribute_unused())
{
	cipher_failure();
	return false;				/* keep compiler quiet */
}

static void
cipher_failure(void)
{
#ifndef FRONTEND
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster file encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use this feature."))));
#else
	fprintf(stderr, _("cluster file encryption is not supported because OpenSSL is not supported by this build"));
	exit(1);
#endif
}
