if [ ! -d "release" ]; then
  mkdir release
fi
dotnet pack src/Chuye.Kafka/Chuye.Kafka.csproj --output ../../release
