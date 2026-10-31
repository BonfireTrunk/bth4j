package today.bonfire.oss.bth4j.admin.http;

final class HttpError extends RuntimeException {

    private final int status;

    HttpError(int status, String message) {
        super(message, null, false, false);
        this.status = status;
    }

    int status() {
        return status;
    }
}
