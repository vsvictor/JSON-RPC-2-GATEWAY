#include <stdio.h>
#include <srt/srt.h>

int main(void) {
    if (srt_startup() != 0) {
        fprintf(stderr, "SRT startup failed: %s\n", srt_getlasterror_str());
        return 1;
    }
    SRTSOCKET socket = srt_create_socket();
    int key_length = 32;
    const char passphrase[] = "gateway-build-check";
    int status = 0;
    if (socket == SRT_INVALID_SOCK ||
        srt_setsockflag(socket, SRTO_PBKEYLEN, &key_length, sizeof(key_length)) != 0 ||
        srt_setsockflag(socket, SRTO_PASSPHRASE, passphrase, sizeof(passphrase) - 1) != 0) {
        fprintf(stderr, "SRT encryption check failed: %s\n", srt_getlasterror_str());
        status = 1;
    }
    if (socket != SRT_INVALID_SOCK) {
        srt_close(socket);
    }
    srt_cleanup();
    return status;
}
