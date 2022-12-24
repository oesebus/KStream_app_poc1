using System.Threading.Tasks;

namespace Oesebus.Order.Domain.Interfaces
{
    public interface IPaymentService
    {
        public Task ProcessPayment();

    }
}
