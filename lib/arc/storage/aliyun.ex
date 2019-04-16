defmodule Arc.Storage.Aliyun do
  require Logger

  alias Arc.Storage.Local

  alias Aliyun.Oss.Object
  @default_expiry_time 60 * 1

  def put(definition, version, {file, scope}) do
    destination_dir = definition.storage_dir(version, {file, scope})
    s3_bucket = s3_bucket(definition)
    s3_key = Path.join(destination_dir, file.file_name)

    s3_options = %{}

    do_put(file, {s3_bucket, s3_key, s3_options})
  end

  def url(definition, version, file_and_scope, _options \\ []) do
    build_signed_url(definition, version, file_and_scope, _options)
  end

  def delete(definition, version, {file, scope}) do
    s3_bucket(definition)
    |> Object.delete_object(s3_key(definition, version, {file, scope}))
  end

  # If the file is stored as a binary in-memory, send to AWS in a single request
  defp do_put(file = %Arc.File{binary: file_binary}, {s3_bucket, s3_key, s3_options})
       when is_binary(file_binary) do
    Object.put_object(s3_bucket, s3_key, file_binary, s3_options)
    |> case do
      {:ok} -> {:ok, file.file_name}
      {:error, error} -> {:error, error}
    end
  end

  # Stream the file and upload to AWS as a multi-part upload
  defp do_put(file, {s3_bucket, s3_key, s3_options}) do
    case File.read(file.path) do
      {:ok, file_binary} ->
        Object.put_object(s3_bucket, s3_key, file_binary, s3_options)
        |> case do
          {:ok} -> {:ok, file.file_name}
          {:error, error} -> {:error, error}
        end

      {:error, error} ->
        {:error, error}
    end

    # rescue
    #   e in ExAws.Error ->
    #     Logger.error(inspect(e))
    #     Logger.error(e.message)
    #     {:error, :invalid_bucket}
  end

  defp build_local_path(definition, version, file_and_scope) do
    Path.join([
      definition.storage_dir(version, file_and_scope),
      Arc.Definition.Versioning.resolve_file_name(definition, version, file_and_scope)
    ])
  end

  defp build_signed_url(definition, version, file_and_scope, options) do
    expires = Timex.now() |> Timex.shift(hours: 1) |> Timex.to_unix()
    Object.object_url(s3_bucket(definition), s3_key(definition, version, file_and_scope), expires)
  end

  defp s3_bucket(definition) do
    case definition.bucket() do
      {:system, env_var} when is_binary(env_var) -> System.get_env(env_var)
      name -> name
    end
  end

  defp s3_key(definition, version, file_and_scope) do
    Path.join([
      definition.storage_dir(version, file_and_scope),
      Arc.Definition.Versioning.resolve_file_name(definition, version, file_and_scope)
    ])
  end
end
