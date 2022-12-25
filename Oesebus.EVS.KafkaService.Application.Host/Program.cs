using Microsoft.Extensions.Hosting;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Oesebus.Order.Application.Host.IoC;
using System;
using Oesebus.EVS.KafkaService.Application.Host.Workers;

namespace Oesebus.Order.Application.Host
{
    static class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
             System.AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionTrapper;
    }

    static void UnhandledExceptionTrapper(object sender, UnhandledExceptionEventArgs e)
    {
      Console.WriteLine(e.ExceptionObject.ToString());
      Console.WriteLine("Press Enter to continue");
      Console.ReadLine();
      Environment.Exit(1);
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>

            Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder(args)
                .ConfigureLogging((builder, logging) => { })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                }).UseConsoleLifetime();
    }
}
