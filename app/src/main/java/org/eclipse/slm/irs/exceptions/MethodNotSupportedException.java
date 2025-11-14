package org.eclipse.slm.irs.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_IMPLEMENTED)
public class MethodNotSupportedException extends RuntimeException {

	public MethodNotSupportedException() {
		super("Method not supported by Information Receiving Service");
	}
}
