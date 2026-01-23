pub trait ResultFutureResultExt<T, E> {
	fn map_async_transpose_flatten<F: FnOnce(T) -> Fut, Fut: Future<Output = Result<U, E>>, U>(
		self,
		f: F,
	) -> impl Future<Output = Result<U, E>>;
}

impl<T, E> ResultFutureResultExt<T, E> for Result<T, E> {
	async fn map_async_transpose_flatten<
		F: FnOnce(T) -> Fut,
		Fut: Future<Output = Result<U, E>>,
		U,
	>(
		self,
		f: F,
	) -> Result<U, E> {
		self.map(f)?.await
	}
}
