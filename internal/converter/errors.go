package converter

import "errors"

var (
	ErrMetadataIsNotSet             = errors.New("metadata is not set")
	ErrEncodingIsNotSet             = errors.New("payload encoding metadata is not set")
	ErrEncodingIsNotSupported       = errors.New("payload encoding is not supported")
	ErrUnableToEncode               = errors.New("unable to encode")
	ErrUnableToDecode               = errors.New("unable to decode")
	ErrUnableToSetValue             = errors.New("unable to set value")
	ErrUnableToFindConverter        = errors.New("unable to find converter")
	ErrTypeNotImplementProtoMessage = errors.New("type doesn't implement proto.Message")
	ErrValuePtrIsNotPointer         = errors.New("not a pointer type")
	ErrValuePtrMustConcreteType     = errors.New("must be a concrete type, not interface")
	ErrTypeIsNotByteSlice           = errors.New("type is not *[]byte")
)
