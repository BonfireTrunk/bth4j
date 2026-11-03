package today.bonfire.oss.bth4j.admin.http;

final class HttpError extends RuntimeException {

    private final int status;
    private final String code;

    HttpError(int status, String message) {
        this(status, message, null);
    }

    HttpError(int status, String message, String code) {
        super(message, null, false, false);
        this.status = status;
        this.code = code;
    }

    String code() {
        return code;
    }

    int status() {
        return status;
    }
}
