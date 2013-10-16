using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vizelia.Storage
{
    public class MD 
    {
        /// <summary>
        /// The Meter id
        /// </summary>
        public int KeyMeter;

        /// <summary>
        /// The MeterData id
        /// </summary>
        public long KeyMeterData;

        /// <summary>
        /// The Acquisition DateTime.
        /// </summary>
        public DateTime AcquisitionDateTime;

        /// <summary>
        /// The Value.
        /// </summary>
        public double Value;

        /// <summary>
        /// The Validity (the value of a validity enum)
        /// </summary>
        public byte Validity;
    }
}
